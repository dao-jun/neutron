package org.daojun.neutron.mledger.inernal;

import io.netty.buffer.ByteBuf;
import org.daojun.neutron.mledger.*;
import org.daojun.neutron.storage.Position;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

public class DefaultManagedLedger implements ManagedLedger {
    private final ConcurrentNavigableMap<Long, Integer> ledgers = new ConcurrentSkipListMap<>();

    public DefaultManagedLedger(String name, ManagedStoreConfig config) {
    }

    @Override
    public CompletableFuture<Position> asyncAddEntry(ByteBuf entry) {
        return null;
    }

    @Override
    public CompletableFuture<Entry> asyncReadEntry(Position position) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Entry>> asyncReadEntries(Collection<Position> positions) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Entry>> asyncReadEntries(Position startPosition, int num) {
        return null;
    }

    @Override
    public CompletableFuture<Collection<Entry>> asyncReadEntries(Position start, int numberOfEntries, int maxBytes,
                                                                 Position maxPosition, Predicate<Long> skipCondition) {
        return null;
    }

    protected CompletableFuture<Collection<Entry>> internalAsyncReadEntries(Position start, Position end) {
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
    public CompletableFuture<Void> asyncFlush() {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncDelete() {
        return null;
    }

    @Override
    public CompletableFuture<ManagedCursor> asyncOpenCursor(String cursorName) {
        return null;
    }

    @Override
    public CompletableFuture<ManagedCursor> getSlowerCursor() {
        return null;
    }

    @Override
    public CompletableFuture<Void> asyncDeleteCursor(String cursorName) {
        return null;
    }

    @Override
    public Position getLastPosition() {
        return null;
    }


    @Override
    public Position nextValidPosition(Position position) {
        return null;
    }
}
