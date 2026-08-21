package me.hsgamer.topper.agent.update;

public class StatusEntry {
    private final UpdateStatus status;
    private final long requestId;

    public StatusEntry(UpdateStatus status, long requestId) {
        this.status = status;
        this.requestId = requestId;
    }

    public UpdateStatus getStatus() {
        return status;
    }

    public long getRequestId() {
        return requestId;
    }
}