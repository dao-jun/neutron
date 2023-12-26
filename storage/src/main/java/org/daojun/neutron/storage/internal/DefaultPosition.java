package org.daojun.neutron.storage.internal;

import org.daojun.neutron.storage.Position;

public class DefaultPosition implements Position {

    private final long ledgerId;
    private final int entryId;

    private DefaultPosition(long ledgerId, int entryId) {
        this.ledgerId = ledgerId;
        this.entryId = entryId;
    }

    public static DefaultPosition create(long ledgerId, int entryId) {
        return new DefaultPosition(ledgerId, entryId);
    }

    @Override
    public long ledgerId() {
        return ledgerId;
    }

    @Override
    public int entryId() {
        return entryId;
    }

    @Override
    public Position next() {
        if (entryId < 0) {
            return DefaultPosition.create(ledgerId, 0);
        } else {
            return DefaultPosition.create(ledgerId, entryId + 1);
        }
    }

    @Override
    public String toString() {
        return "DefaultPosition{" +
                "ledgerId=" + ledgerId +
                ", entryId=" + entryId +
                '}';
    }
}
