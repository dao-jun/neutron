package org.daojun.neutron.mledger;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.storage.Position;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ManagedCursor {

    void rewind();

    void seek(Position position, boolean force);

    CompletableFuture<Collection<Entry>> asyncReadEntries(int number);

    CompletableFuture<Void> asyncDelete(Position position);

    CompletableFuture<Void> asyncDelete(Collection<Position> entryIds);

    CompletableFuture<Void> asyncMarkDelete(Position position);

    default CompletableFuture<Collection<Entry>> asyncReadEntries(int numberOfEntries, int maxBytes,
                                                                    Position maxPosition) {
        return asyncReadEntries(numberOfEntries, maxBytes, maxPosition, (position) -> false);
    }

    CompletableFuture<Collection<Entry>> asyncReadEntries(int numberOfEntries, int maxBytes, Position maxPosition,
                                                            Predicate<Long> skipCondition);

    CompletableFuture<Void> asyncClose();

    String getName();

    boolean hasMoreEntries();

    CompletableFuture<Long> asyncGetBacklogEntries();

    CompletableFuture<Long> asyncGetBacklogBytes();

    CompletableFuture<Long> asyncGetNumberOfEntries();

    CompletableFuture<Long> asyncGetNumberOfBytes();

    CompletableFuture<Long> asyncGetReadPosition();

    CompletableFuture<Long> asyncGetMarkDeletePosition();

    CompletableFuture<Void> asyncClearBacklog();

    CompletableFuture<Void> asyncSkipEntries(int numberOfEntriesToSkip);

    CompletableFuture<Void> asyncResetCursor(Position position);

    ManagedLedger getLedger();

    // Remove all entries that have been deleted.
    void trimDeletedEntries(Collection<ByteBuf> entries);

    boolean deleted(Position position);

}
