package org.daojun.neutron.mledger;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.storage.Position;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

public interface ManagedLedger {

    CompletableFuture<Position> asyncAddEntry(ByteBuf entry);

    CompletableFuture<Entry> asyncReadEntry(Position position);

    CompletableFuture<Collection<Entry>> asyncReadEntries(Collection<Position> positions);

    CompletableFuture<Collection<Entry>> asyncReadEntries(Position startPosition, int num);

    CompletableFuture<Collection<Entry>> asyncReadEntries(Position start, int numberOfEntries, int maxBytes,
                                                          Position maxPosition, Predicate<Long> skipCondition);

    CompletableFuture<Long> asyncGetNumberOfEntries();

    CompletableFuture<Long> asyncGetNumberOfBytes();

    CompletableFuture<Void> asyncFlush();

    CompletableFuture<Void> asyncClose();

    CompletableFuture<Void> asyncDelete();

    CompletableFuture<ManagedCursor> asyncOpenCursor(String cursorName);

    CompletableFuture<ManagedCursor> getSlowerCursor();

    CompletableFuture<Void> asyncDeleteCursor(String cursorName);

    Position getLastPosition();


    Position nextValidPosition(Position position);
}
