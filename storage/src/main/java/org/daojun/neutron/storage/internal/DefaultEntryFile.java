package org.daojun.neutron.storage.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.daojun.neutron.storage.EntryFile;
import org.daojun.neutron.storage.FencedException;
import org.daojun.neutron.storage.ManagedStorageConfig;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * Write/Read entries by FileChannel, without any synchronization and cache.
 */
@Slf4j
public class DefaultEntryFile implements EntryFile {
    // First 4 bytes: magic number
    // Second 4 bytes: wrote position
    // Third 4 bytes: flushed position
    private static final int HEADER_SIZE = 4 + 4 + 4;
    private static final int MAGIC = 0x12345678;
    private static final AtomicReferenceFieldUpdater<DefaultEntryFile, DefaultEntryFile.State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DefaultEntryFile.class, DefaultEntryFile.State.class, "state");

    private final long id;
    private final String name;
    private final String directory;
    private final File file;
    private final int threshold;
    private FileChannel fc;
    private final int maxCache;
    private boolean needRecover = true;

    // Is the entry file read only?
    private volatile boolean readonly = false;
    // The wrote position
    private final AtomicInteger wrote = new AtomicInteger();
    // The flushed position
    private final AtomicInteger flushed = new AtomicInteger();
    private final ByteBufAllocator allocator = ByteBufAllocator.DEFAULT;
    // pending operations
    private final AtomicInteger pendingOps = new AtomicInteger(0);

    private final ScheduledExecutorService scheduler;
    // How many bytes in the buffer
    private final AtomicInteger cached = new AtomicInteger(0);
    // The buffer for entries
    private final NavigableMap<Integer, ByteBuf> buffer = new ConcurrentSkipListMap<>(Integer::compareTo);

    private volatile DefaultEntryFile.State state = DefaultEntryFile.State.NEW;
    // The future for initialization
    private final CompletableFuture<Void> initializeFuture = new CompletableFuture<>();

    private enum State {
        NEW,
        INITIALIZING,
        INITIALIZE_FAILED,
        INITIALIZED,
        FENCED
    }


    public DefaultEntryFile(ManagedStorageConfig config, String directory, long id, ScheduledExecutorService scheduler) throws IOException {
        this.id = id;
        this.name = String.valueOf(id);
        this.directory = directory;
        this.scheduler = scheduler;
        this.file = FileUtils.getFile(directory, name);
        FileUtils.createParentDirectories(file);
        if (!this.file.exists()) {
            needRecover = false;
            this.file.createNewFile();
        }
        this.threshold = config.getMaxEntryFileSize();
        this.maxCache = config.getMaxEntryCacheOfOneFile();
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
    public String path() {
        return this.file.getPath();
    }

    @Override
    public int size() {
        return this.wrote.get();
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    @Override
    public CompletableFuture<Void> asyncInitialize() {
        if (state.equals(State.INITIALIZE_FAILED) || state.equals(State.INITIALIZED)
                || state.equals(State.INITIALIZING) || !STATE_UPDATER.compareAndSet(this, State.NEW, State.INITIALIZING)) {
            return initializeFuture;
        }
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new FencedException("Entry file is fenced"));
        }

        var hasException = false;
        try {
            this.fc = FileChannel.open(file.toPath(), StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (needRecover) {
                readonly = true;
                recover();
            } else {
                wrote.addAndGet(HEADER_SIZE);
                flushed.addAndGet(HEADER_SIZE);
                persist();
            }
            STATE_UPDATER.set(this, State.INITIALIZED);
            initializeFuture.complete(null);
        } catch (IOException e) {
            hasException = true;
            STATE_UPDATER.set(this, State.INITIALIZE_FAILED);
            initializeFuture.completeExceptionally(e);
        } finally {
            if (fc != null && hasException) {
                try {
                    fc.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
        return initializeFuture;
    }

    /**
     * Recover the entry file.
     *
     * @throws IOException
     */
    private void recover() throws IOException {
        var fc = this.fc;
        if (fc == null) {
            throw new IllegalStateException("Entry file is not opened");
        }

        var b = ByteBuffer.allocate(HEADER_SIZE);
        fc.read(b, 0);
        b.flip();
        var magic = b.getInt();
        if (magic != MAGIC) {
            throw new IllegalStateException("Invalid magic number");
        }
        var wrote = b.getInt();
        var flushed = b.getInt();
        if (flushed < 0 || wrote != flushed) {
            throw new IllegalStateException("Invalid wrote or flushed position");
        }
        this.wrote.set(wrote);
        this.flushed.set(flushed);
    }

    /**
     * Persist the entry file's metadata.
     *
     * @throws IOException
     */
    private void persist() throws IOException {
        var b = ByteBuffer.allocate(HEADER_SIZE);
        b.putInt(MAGIC);
        b.putInt(wrote.get());
        b.putInt(flushed.get());
        b.flip();
        fc.write(b, 0);
        fc.force(true);
    }

    @Override
    @SuppressWarnings("all")
    public synchronized CompletableFuture<Void> asyncFlush() {
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new FencedException("Entry file is fenced"));
        }
        return initializeFuture
                .thenCompose(__ -> {
                    if (isReadOnly()) {
                        return CompletableFuture.completedFuture(null);
                    }
                    var f = new CompletableFuture<Void>();
                    try {
                        pendingOps.incrementAndGet();
                        if (buffer.isEmpty() || wrote.get() == flushed.get()) {
                            f.complete(null);
                            return f;
                        }
                        var entry = buffer.firstEntry();
                        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                        flushInternal(entry, sizeBuffer);
                        persist();
                        f.complete(null);
                    } catch (Exception ex) {
                        f.completeExceptionally(ex);
                    } finally {
                        pendingOps.decrementAndGet();
                    }
                    return f;
                });
    }

    @SuppressWarnings("all")
    private void flushInternal(Map.Entry<Integer, ByteBuf> entry, ByteBuffer sizeBuffer) throws IOException {
        while (entry != null && entry.getKey() == flushed.get()) {
            var key = entry.getKey();
            var value = entry.getValue();

            if (value.refCnt() <= 0) {
                buffer.remove(entry.getKey());
                continue;
            }
            var size = value.readableBytes();
            flushed.getAndAdd(size + 4);
            fc.write(sizeBuffer.putInt(size).flip(), key);
            fc.write(value.nioBuffer(), key + 4);
            sizeBuffer.clear();
            value.release();
            buffer.remove(entry.getKey());
            cached.addAndGet(-size - 4);
            entry = buffer.firstEntry();
        }
        fc.force(true);
    }

    @Override
    public synchronized CompletableFuture<Void> asyncClose() {
        return asyncFlush()
                .thenCompose(__ -> {
                    if (state.equals(State.FENCED)) {
                        return CompletableFuture.completedFuture(null);
                    }

                    STATE_UPDATER.set(this, State.FENCED);
                    // Block until all pending operations are completed.
                    while (pendingOps.get() > 0) {
                        LockSupport.parkNanos(1000);
                    }
                    var f = new CompletableFuture<Void>();
                    try {
                        fc.close();
                        f.complete(null);
                    } catch (IOException e) {
                        f.completeExceptionally(e);
                    }
                    return f;
                });
    }

    @Override
    public CompletableFuture<ByteBuf> asyncReadEntry(int offset) {
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new FencedException("Entry file is fenced"));
        }

        return initializeFuture
                .thenCompose(__ -> {
                    var f = new CompletableFuture<ByteBuf>();
                    try {
                        pendingOps.incrementAndGet();
                        if (offset < HEADER_SIZE || offset > wrote.get()) {
                            f.completeExceptionally(new IllegalArgumentException("Offset out of range"));
                        } else if (offset < flushed.get()) {
                            // Read from file channel
                            var sizeBuffer = ByteBuffer.allocate(4);
                            fc.read(sizeBuffer, offset);
                            sizeBuffer.flip();
                            var size = sizeBuffer.getInt();
                            var buffer = ByteBuffer.allocate(size);
                            fc.read(buffer, offset + 4);
                            buffer.flip();
                            f.complete(allocator.buffer(size).writeBytes(buffer));
                        } else if (offset >= flushed.get() && offset < wrote.get() && buffer.containsKey(offset)) {
                            // Read from buffer
                            var entry = buffer.get(offset);
                            f.complete(entry.retainedSlice());
                        } else {
                            f.completeExceptionally(new IllegalStateException("Entry not found"));
                        }
                    } catch (IOException ex) {
                        f.completeExceptionally(ex);
                    } finally {
                        pendingOps.decrementAndGet();
                    }
                    return f;
                });
    }

    @Override
    public CompletableFuture<Integer> asyncAddEntry(ByteBuf b) {
        if (isReadOnly()) {
            return CompletableFuture.completedFuture(-1);
        }
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new FencedException("Entry file is fenced"));
        }

        flushIfNeeded();
        return initializeFuture
                .thenCompose(__ -> {
                    try {
                        var f = new CompletableFuture<Integer>();
                        pendingOps.incrementAndGet();
                        var size = b.readableBytes();
                        if (wrote.get() > threshold) {
                            f.complete(-1);
                            return f;
                        }
                        var offset = wrote.getAndAdd(size + 4);
                        // Roll next file.
                        if (offset > threshold) {
                            // Make sure the wrote position is correct.
                            wrote.addAndGet(-size - 4);
                            f.complete(-1);
                            return f;
                        }
                        buffer.put(offset, b.retain());
                        cached.addAndGet(size + 4);
                        f.complete(offset);
                        return f;
                    } finally {
                        pendingOps.decrementAndGet();
                    }
                });
    }


    private void flushIfNeeded() {
        if (cached.get() < maxCache) {
            return;
        }
        try {
            asyncFlush().get();
        } catch (Exception e) {
            log.error("Failed to flush entry file {}", id, e);
        }
    }

    @Override
    public CompletableFuture<Void> asyncDelete() {
        return asyncClose()
                .thenCompose(__ -> {
                    try {
                        FileUtils.forceDelete(file);
                        return CompletableFuture.completedFuture(null);
                    } catch (IOException e) {
                        return CompletableFuture.failedFuture(e);
                    }
                });
    }
}
