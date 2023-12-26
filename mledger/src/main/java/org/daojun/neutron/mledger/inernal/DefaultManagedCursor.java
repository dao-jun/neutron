package org.daojun.neutron.mledger.inernal;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.mledger.Entry;
import org.daojun.neutron.mledger.ManagedCursor;
import org.daojun.neutron.mledger.ManagedLedger;
import org.daojun.neutron.storage.Position;

import java.util.Collection;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

public class DefaultManagedCursor implements ManagedCursor {

    private final String name;
    private final ManagedLedger ledger;
    private volatile Position read;
    private volatile Position markDeleted;
    private final ConcurrentSkipListSet<Position> individualDeleted = new ConcurrentSkipListSet<>(Position::compareTo);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // For read operations
    private final Lock readLock = lock.readLock();
    // For update read/markDelete/individualDeleted operations
    private final Lock writeLock = lock.writeLock();

    public DefaultManagedCursor(String name, ManagedLedger store) {
        this.name = name;
        this.ledger = store;
    }


    @Override
    public void rewind() {
        this.writeLock.lock();
        try {
            Position position = this.markDeleted;
            if (position == null) {
                position = Position.EARLIEST;
            }
            this.read = this.ledger.nextValidPosition(position);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void seek(Position position, boolean force) {
        this.writeLock.lock();
        try {
            if (!force && position.compareTo(this.markDeleted) <= 0) {
                position = ledger.nextValidPosition(position);
            }
            this.read = position;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Collection<Entry>> asyncReadEntries(int number) {
        return this.ledger.asyncReadEntries(read, number);
    }

    @Override
    public CompletableFuture<Void> asyncDelete(Position position) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        this.writeLock.lock();
        try {
            if (position.compareTo(this.markDeleted) <= 0) {
                f.completeExceptionally(new IllegalArgumentException("Position is less than markDeleted"));
                return f;
            }
            if (this.individualDeleted.contains(position)) {
                f.completeExceptionally(new IllegalArgumentException("Position has been deleted"));
                return f;
            }
            this.individualDeleted.add(position);
            f.complete(null);
        } finally {
            this.writeLock.unlock();
        }
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncDelete(Collection<Position> entryIds) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        this.writeLock.lock();
        try {
            for (Position position : entryIds) {
                if (position.compareTo(this.markDeleted) <= 0) {
                    f.completeExceptionally(new IllegalArgumentException("Position is less than markDeleted"));
                    return f;
                }
                if (this.individualDeleted.contains(position)) {
                    f.completeExceptionally(new IllegalArgumentException("Position has been deleted"));
                    return f;
                }
                this.individualDeleted.add(position);
            }
            f.complete(null);
        } finally {
            this.writeLock.unlock();
        }
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncMarkDelete(Position position) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        this.writeLock.lock();
        try {
            if (position.compareTo(this.markDeleted) <= 0) {
                f.completeExceptionally(new IllegalArgumentException("Position is less than markDeleted"));
                return f;
            }
            NavigableSet<Position> toDeleted = this.individualDeleted.headSet(position, true);
            if (toDeleted.size() > 0) {
                this.individualDeleted.removeAll(toDeleted);
            }
            this.markDeleted = position;
            f.complete(null);
        } finally {
            this.writeLock.unlock();
        }
        return f;
    }

    @Override
    public CompletableFuture<Collection<Entry>> asyncReadEntries(int numberOfEntries, int maxBytes,
                                                                 Position maxPosition, Predicate<Long> skipCondition) {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        // TODO must take snapshot of markDelete and individualDeleted
        return null;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public boolean hasMoreEntries() {
        return this.read.compareTo(this.ledger.getLastPosition()) < 0;
    }

    @Override
    public CompletableFuture<Long> asyncGetBacklogEntries() {
        return null;
    }

    @Override
    public CompletableFuture<Long> asyncGetBacklogBytes() {
        return null;
    }

    @Override
    public CompletableFuture<Long> asyncGetNumberOfEntries() {
        return null;
    }

    @Override
    public CompletableFuture<Long> asyncGetNumberOfBytes() {
        return null;
    }

    @Override
    public CompletableFuture<Long> asyncGetReadPosition() {
        return null;
    }

    @Override
    public CompletableFuture<Long> asyncGetMarkDeletePosition() {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncClearBacklog() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        this.writeLock.lock();
        try {
            Position lastPosition = this.ledger.getLastPosition();
            this.individualDeleted.clear();
            this.markDeleted = lastPosition;
            this.read = lastPosition;
            f.complete(null);
        } finally {
            this.writeLock.unlock();
        }
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncSkipEntries(int numberOfEntriesToSkip) {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncResetCursor(Position position) {
        return null;
    }

    @Override
    public ManagedLedger getLedger() {
        return null;
    }

    @Override
    public void trimDeletedEntries(Collection<ByteBuf> entries) {

    }

    @Override
    public boolean deleted(Position position) {
        if (position.compareTo(this.markDeleted) <= 0) {
            return true;
        }
        return this.individualDeleted.contains(position);
    }
}
