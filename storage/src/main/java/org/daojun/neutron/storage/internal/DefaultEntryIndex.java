package org.daojun.neutron.storage.internal;

import org.daojun.neutron.storage.*;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultEntryIndex implements EntryIndex {
    private final ManagedStorageConfig config;
    private final String path;
    public RocksDB index;
    private WriteOptions syncOp;
    private WriteOptions asyncOp;
    private WriteBatch emptyBatch;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public DefaultEntryIndex(ManagedStorageConfig config, String directory) {
        this.config = config;
        this.path = directory + "/index";
        try {
            Files.createDirectories(Path.of(path));
            initialize();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void initialize() throws RocksDBException {
        try {
            RocksDB.loadLibrary();
            syncOp = new WriteOptions().setSync(true);
            asyncOp = new WriteOptions().setSync(false);
            emptyBatch = new WriteBatch();
        } catch (Exception ex) {
            throw new IllegalStateException("Unable to load rocksdb");
        }
        this.index = RocksDB.open(this.path);
    }

    @Override
    public CompletableFuture<Void> asyncAdd(Position position, LongIntPair pair) {
        var f = new CompletableFuture<Void>();

        var key = LongIntBytes.get(position.ledgerId(), position.entryId());
        var value = LongIntBytes.get(pair.f1, pair.f2);
        try {
            index.put(asyncOp, key.memory, value.memory);
            f.complete(null);
        } catch (Exception ex) {
            f.completeExceptionally(ex);
        } finally {
            key.recycle();
            value.recycle();
        }
        return f;
    }


    @Override
    public CompletableFuture<Void> asyncAddBatch(List<Pair<Position, LongIntPair>> positions) {
        var f = new CompletableFuture<Void>();
        try (var batch = new WriteBatch()) {
            buildAddBatch(batch, positions);
            index.write(asyncOp, batch);
            f.complete(null);
        } catch (Exception ex) {
            f.completeExceptionally(ex);
        }
        return f;
    }

    private void buildAddBatch(WriteBatch batch, List<Pair<Position, LongIntPair>> positions) throws RocksDBException {
        for (var pair : positions) {
            var key = pair.f1;
            var value = pair.f2;
            var _key = LongIntBytes.get(key.ledgerId(), key.entryId());
            var _value = LongIntBytes.get(value.f1, value.f2);
            try {
                batch.put(_key.memory, _value.memory);
            } finally {
                _key.recycle();
                _value.recycle();
            }
        }
    }

    @Override
    public CompletableFuture<LongIntPair> asyncGet(Position position) {
        var f = new CompletableFuture<LongIntPair>();

        var key = LongIntBytes.get(position.ledgerId(), position.entryId());
        var value = LongIntBytes.get();
        try {
            var ret = index.get(key.memory, value.memory);
            f.complete(ret < 0 ? null : LongIntPair.create(value.f1(), value.f2()));
        } catch (RocksDBException ex) {
            f.completeExceptionally(ex);
        } finally {
            key.recycle();
            value.recycle();
        }
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncDelete(long ledgerId) {
        var f = new CompletableFuture<Void>();
        // Build Keys
        var startKey = LongIntBytes.get(ledgerId, 0);
        var endKey = LongIntBytes.get(ledgerId, Integer.MAX_VALUE);
        try {
            index.deleteRange(asyncOp, startKey.memory, endKey.memory);
            f.complete(null);
        } catch (Exception ex) {
            f.completeExceptionally(ex);
        } finally {
            startKey.recycle();
            endKey.recycle();
        }

        return f;
    }

    @Override
    public CompletableFuture<Void> asyncFlush() {
        var f = new CompletableFuture<Void>();
        try {
            index.write(syncOp, emptyBatch);
            f.complete(null);
        } catch (Exception ex) {
            f.completeExceptionally(ex);
        }

        return f;
    }

    @Override
    public CompletableFuture<Void> asyncClose() {
        if (!this.closed.compareAndSet(false, true)) {
            return CompletableFuture.completedFuture(null);
        }
        return asyncFlush()
                .thenCompose(__ -> {
                    CompletableFuture<Void> f = new CompletableFuture<>();
                    try {
                        this.index.close();
                        this.syncOp.close();
                        this.asyncOp.close();
                        this.emptyBatch.close();
                        f.complete(null);
                    } catch (Exception ex) {
                        f.completeExceptionally(ex);
                    }
                    return f;
                });
    }

    @Override
    public boolean closed() {
        return this.closed.get();
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public CompletableFuture<Long> asyncGetCount() {
        var f = new CompletableFuture<Long>();

        try {
            f.complete(this.index.getLongProperty("rocksdb.estimate-num-keys"));
        } catch (RocksDBException e) {
            f.completeExceptionally(e);
        }
        return f;
    }

    @Override
    public CompletableFuture<Void> asyncCompact() {
        var f = new CompletableFuture<Void>();
        try {
            this.index.compactRange();
            f.complete(null);
        } catch (Exception ex) {
            f.completeExceptionally(ex);
        }
        return f;
    }
}
