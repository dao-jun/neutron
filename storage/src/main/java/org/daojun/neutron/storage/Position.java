package org.daojun.neutron.storage;

import org.daojun.neutron.storage.internal.DefaultPosition;

public interface Position {
    Position EARLIEST = DefaultPosition.create(-1, -1);
    Position LATEST = DefaultPosition.create(Long.MAX_VALUE, Integer.MAX_VALUE);


    long ledgerId();

    int entryId();

    Position next();

    default int compareTo(Position position) {
        if (this.ledgerId() < position.ledgerId()) {
            return -1;
        } else if (this.ledgerId() > position.ledgerId()) {
            return 1;
        } else {
            return Long.compare(this.entryId(), position.entryId());
        }
    }

}
