package me.hsgamer.topper.agent.update;

import me.hsgamer.topper.agent.core.DataEntryAgent;
import me.hsgamer.topper.data.core.DataEntry;
import me.hsgamer.topper.data.core.DataHolder;
import me.hsgamer.topper.value.core.ValueWrapper;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class UpdateAgent<K, V> implements DataEntryAgent<K, V> {
    private final BiConsumer<K, Consumer<ValueWrapper<V>>> updateConsumer;

    private final Map<K, SetEntry> map = new ConcurrentHashMap<>();
    private final Map<IdentityKey<DataEntry<K, V>>, SetEntry> entryMap = new ConcurrentHashMap<>();
    private final Queue<SetEntry> pendingSetQueue = new ConcurrentLinkedQueue<>();

    private final Queue<SetEntry> deferredSetQueue = new ConcurrentLinkedQueue<>();

    private Function<K, FilterResult> filter = null;
    private BiFunction<K, ValueWrapper<V>, ValueWrapper<V>> errorHandler = null;
    private int maxSkips = 1;

    public UpdateAgent(DataHolder<K, V> holder, BiConsumer<K, Consumer<ValueWrapper<V>>> updateConsumer) {
        this.updateConsumer = updateConsumer;
    }

    public void setFilter(Function<K, FilterResult> filter) {
        this.filter = filter;
    }

    public void setErrorHandler(BiFunction<K, ValueWrapper<V>, ValueWrapper<V>> errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void setErrorHandler(BiConsumer<K, ValueWrapper<V>> errorHandler) {
        setErrorHandler((k, v) -> {
            errorHandler.accept(k, v);
            return v;
        });
    }

    public void setMaxSkips(int maxSkips) {
        this.maxSkips = maxSkips;
    }

    private void setValueEntry(ValueWrapper<V> valueWrapper, SetEntry entry, StatusEntry expectedStatus) {
        if (errorHandler != null && valueWrapper.state == ValueWrapper.State.ERROR) {
            valueWrapper = errorHandler.apply(entry.key, valueWrapper);
        }

        UpdateStatus nextStatus;
        switch (valueWrapper.state) {
            case ERROR:
            case NOT_HANDLED:
                nextStatus = new UpdateStatus.Skip(maxSkips);
                break;
            default:
                nextStatus = new UpdateStatus.Set(valueWrapper.value);
                break;
        }

        compareAndSetStatus(entry, expectedStatus, nextStatus, expectedStatus.getRequestId());
    }

    public Runnable getUpdateRunnable(int maxEntryPerCall) {
        return new Runnable() {
            private final AtomicReference<Iterator<Map.Entry<K, SetEntry>>> iteratorRef = new AtomicReference<>();
            private final AtomicBoolean running = new AtomicBoolean(false);

            @Override
            public void run() {
                if (!running.compareAndSet(false, true)) {
                    return;
                }

                try {
                    Iterator<Map.Entry<K, SetEntry>> iterator = iteratorRef.updateAndGet(old ->
                            old == null || !old.hasNext() ? map.entrySet().iterator() : old
                    );
                    int count = 0;
                    while (count < maxEntryPerCall && iterator.hasNext()) {
                        Map.Entry<K, SetEntry> mapEntry;
                        try {
                            mapEntry = iterator.next();
                        } catch (Exception e) {
                            break;
                        }

                        SetEntry entry = mapEntry.getValue();
                        if (!entry.active.get()) {
                            continue;
                        }

                        StatusEntry statusEntry = entry.status.get();
                        UpdateStatus updateStatus = statusEntry.getStatus() ;

                        if (updateStatus instanceof UpdateStatus.Value) {
                            ValueWrapper<V> valueWrapper = ((UpdateStatus.Value) updateStatus).get();
                            setValueEntry(valueWrapper, entry, statusEntry);
                            continue;
                        }

                        if (updateStatus != UpdateStatus.DEFAULT && !(updateStatus instanceof UpdateStatus.Set)) {
                            continue;
                        }

                        if (filter != null) {
                            FilterResult filterResult = filter.apply(entry.key);
                            switch (filterResult) {
                                case SKIP:
                                    compareAndSetStatus(entry, statusEntry, new UpdateStatus.Skip(maxSkips), 0);
                                    continue;
                                case RESET:
                                    compareAndSetStatus(entry, statusEntry, UpdateStatus.RESET, 0);
                                    continue;
                                case CONTINUE:
                                    // Do nothing, continue to update.
                                    break;
                            }
                        }

                        long requestId = entry.nextRequestId.incrementAndGet();
                        if (!compareAndSetStatus(entry, statusEntry, UpdateStatus.UPDATING, requestId)) {
                            continue;
                        }

                        updateConsumer.accept(entry.key, valueWrapper -> completeUpdate(entry, requestId, valueWrapper));

                        StatusEntry afterUpdateStatus = entry.status.get();
                        if (afterUpdateStatus.getStatus() instanceof UpdateStatus.Value
                                && afterUpdateStatus.getRequestId() == requestId) {
                            ValueWrapper<V> valueWrapper = ((UpdateStatus.Value) afterUpdateStatus.getStatus() ).get();
                            setValueEntry(valueWrapper, entry, afterUpdateStatus);
                        }

                        count++;
                    }
                } finally {
                    running.set(false);
                }
            }
        };
    }

    public Runnable getSetRunnable() {
        return () -> {
            activateDeferredSetEntries();

            SetEntry entry;
            while ((entry = pendingSetQueue.poll()) != null) {
                processSetEntry(entry);
            }
        };
    }

    private void activateDeferredSetEntries() {
        SetEntry entry;
        while ((entry = deferredSetQueue.poll()) != null) {
            if (entry.active.get()) {
                pendingSetQueue.offer(entry);
            } else {
                entry.queued.set(false);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processSetEntry(SetEntry entry) {
        if (!entry.active.get()) {
            entry.queued.set(false);
            return;
        }

        StatusEntry currentStatus = entry.status.get();
        UpdateStatus updateStatus = currentStatus.getStatus();

        if (updateStatus instanceof UpdateStatus.Skip) {
            UpdateStatus.Skip skipStatus = (UpdateStatus.Skip) updateStatus;
            UpdateStatus nextStatus = skipStatus.skip() ? skipStatus.decrement() : UpdateStatus.DEFAULT;
            if (compareAndSetStatus(entry, currentStatus, nextStatus, currentStatus.getRequestId())
                    && nextStatus instanceof UpdateStatus.Skip) {
                deferSetEntry(entry);
            } else {
                requeueOrReleaseSetEntry(entry);
            }
            return;
        }

        if (updateStatus == UpdateStatus.RESET) {
            if (!entry.active.get()) {
                entry.queued.set(false);
                return;
            }

            entry.dataEntry.setValue((V) null);
            if (compareAndSetStatus(entry, currentStatus, new UpdateStatus.Skip(maxSkips), currentStatus.getRequestId())) {
                deferSetEntry(entry);
            } else {
                requeueOrReleaseSetEntry(entry);
            }
            return;
        }

        if (updateStatus instanceof UpdateStatus.Set) {
            V value = (V) ((UpdateStatus.Set) updateStatus).getValue();
            if (!entry.active.get()) {
                entry.queued.set(false);
                return;
            }

            entry.dataEntry.setValue(value);
            compareAndSetStatus(entry, currentStatus, UpdateStatus.DEFAULT, currentStatus.getRequestId());
            requeueOrReleaseSetEntry(entry);
            return;
        }

        entry.queued.set(false);
        requeueOrReleaseSetEntry(entry);
    }

    private void deferSetEntry(SetEntry entry) {
        if (entry.active.get()) {
            deferredSetQueue.offer(entry);
        } else {
            entry.queued.set(false);
        }
    }

    private void requeueOrReleaseSetEntry(SetEntry entry) {
        entry.queued.set(false);

        if (entry.active.get() && isSetPending(entry.status.get().getStatus())) {
            enqueueSetEntry(entry);
        }
    }

    private void enqueueSetEntry(SetEntry entry) {
        if (!entry.active.get() || !isSetPending(entry.status.get().getStatus())) {
            return;
        }

        if (entry.queued.compareAndSet(false, true)) {
            pendingSetQueue.offer(entry);
        }
    }

    private boolean isSetPending(UpdateStatus status) {
        return status == UpdateStatus.RESET || status instanceof UpdateStatus.Skip || status instanceof UpdateStatus.Set;
    }

    private boolean compareAndSetStatus(SetEntry entry, StatusEntry expected, UpdateStatus nextStatus, long requestId) {
        if (!entry.active.get()) {
            return false;
        }

        boolean changed = entry.status.compareAndSet(expected, new StatusEntry(nextStatus, requestId));
        if (changed) {
            enqueueSetEntry(entry);
        }
        return changed;
    }

    private void completeUpdate(SetEntry entry, long requestId, ValueWrapper<V> valueWrapper) {
        if (!entry.active.get()) {
            return;
        }

        while (entry.active.get()) {
            StatusEntry current = entry.status.get();
            if (current.getRequestId() != requestId || !canAcceptUpdateResult(current.getStatus())) {
                return;
            }

            StatusEntry next = new StatusEntry(new UpdateStatus.Value(valueWrapper), requestId);
            if (entry.status.compareAndSet(current, next)) {
                return;
            }
        }
    }

    private boolean canAcceptUpdateResult(UpdateStatus status) {
        return status == UpdateStatus.UPDATING
                || status instanceof UpdateStatus.Value
                || status instanceof UpdateStatus.Set
                || status == UpdateStatus.DEFAULT;
    }

    @Override
    public void onCreate(DataEntry<K, V> dataEntry) {
        SetEntry entry = new SetEntry(dataEntry);
        SetEntry previous = map.put(dataEntry.getKey(), entry);
        entryMap.put(new IdentityKey<>(dataEntry), entry);

        if (previous != null && previous != entry) {
            previous.deactivate();
            entryMap.remove(new IdentityKey<>(previous.dataEntry), previous);
        }
    }

    @Override
    public void onRemove(DataEntry<K, V> dataEntry) {
        SetEntry entry = entryMap.remove(new IdentityKey<>(dataEntry));
        if (entry == null) {
            return;
        }

        entry.deactivate();
        map.remove(dataEntry.getKey(), entry);
    }

    public enum FilterResult {
        SKIP,
        RESET,
        CONTINUE
    }

    private class SetEntry {
        private final K key;
        private final DataEntry<K, V> dataEntry;
        private final AtomicReference<StatusEntry> status = new AtomicReference<>(new StatusEntry(UpdateStatus.DEFAULT, 0));
        private final AtomicBoolean queued = new AtomicBoolean(false);
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicLong nextRequestId = new AtomicLong();

        private SetEntry(DataEntry<K, V> dataEntry) {
            this.key = dataEntry.getKey();
            this.dataEntry = dataEntry;
        }

        private void deactivate() {
            active.set(false);
            queued.set(false);
        }
    }
}
