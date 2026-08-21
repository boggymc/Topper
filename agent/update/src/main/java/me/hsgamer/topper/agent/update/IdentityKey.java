package me.hsgamer.topper.agent.update;

public class IdentityKey<T> {
    private final T value;
    private final int hash;

    public IdentityKey(T value) {
        this.value = value;
        this.hash = System.identityHashCode(value);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof IdentityKey && value == ((IdentityKey<?>) other).value;
    }

    @Override
    public int hashCode() {
        return hash;
    }
}