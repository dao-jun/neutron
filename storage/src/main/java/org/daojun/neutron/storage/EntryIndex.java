package org.daojun.neutron.storage;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface EntryIndex {

    CompletableFuture<Void> asyncAdd(Position key, LongIntPair value);

    CompletableFuture<Void> asyncAddBatch(List<Pair<Position, LongIntPair>> positions);

    CompletableFuture<LongIntPair> asyncGet(Position key);

    CompletableFuture<Void> asyncDelete(long ledgerId);

    CompletableFuture<Void> asyncFlush();

    CompletableFuture<Void> asyncClose();

    boolean closed();

    String getPath();

    CompletableFuture<Long> asyncGetCount();

    CompletableFuture<Void> asyncCompact();

}
