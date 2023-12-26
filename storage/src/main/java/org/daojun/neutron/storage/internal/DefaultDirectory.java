package org.daojun.neutron.storage.internal;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.daojun.neutron.storage.Directory;
import org.daojun.neutron.storage.EntryFile;
import org.daojun.neutron.storage.LongIntPair;
import org.daojun.neutron.storage.ManagedStorageConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

@Slf4j
public class DefaultDirectory implements Directory {
    private static final Map<Long, CompletableFuture<Void>> PENDING_CREATE = new ConcurrentHashMap<>();
    private static final AtomicReferenceFieldUpdater<DefaultDirectory, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DefaultDirectory.class, State.class, "state");
    private static final AtomicReferenceFieldUpdater<DefaultDirectory, EntryFile> CURRENT_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(DefaultDirectory.class, EntryFile.class, "current");

    private final String path;
    private volatile EntryFile current;
    private final AtomicLong idGenerator;
    private final ManagedStorageConfig config;
    private volatile State state = State.NEW;
    private final ScheduledExecutorService scheduler;
    private final CompletableFuture<Void> initializeFuture = new CompletableFuture<>();
    private final ConcurrentNavigableMap<Long, EntryFile> files = new ConcurrentSkipListMap<>(Long::compareTo);


    public DefaultDirectory(ManagedStorageConfig config, String path, AtomicLong idGenerator,
                            ScheduledExecutorService scheduler) {
        this.path = path;
        this.config = config;
        this.idGenerator = idGenerator;
        this.scheduler = scheduler;
    }


    private enum State {
        NEW,
        INITIALIZING,
        INITIALIZE_FAILED,
        INITIALIZED,
        FENCED
    }

    @Override
    public long used() {
        long used = 0;
        for (var entry : files.entrySet()) {
            used += entry.getValue().size();
        }
        return used;
    }

    @Override
    public long allowed() {
        return 0;
    }

    @Override
    public String path() {
        return path;
    }

    @Override
    public boolean containsFile(long fileId) {
        return this.files.containsKey(fileId);
    }

    @Override
    public EntryFile lastEntryFile() {
        var entry = this.files.lastEntry();
        return entry == null ? null : entry.getValue();
    }

    @Override
    public Collection<EntryFile> entryFiles() {
        return this.files.values();
    }

    @Override
    public CompletableFuture<Void> asyncInitialize() {
        if (state.equals(State.INITIALIZE_FAILED) || state.equals(State.INITIALIZED)
                || state.equals(State.INITIALIZING) || !STATE_UPDATER.compareAndSet(this, State.NEW, State.INITIALIZING)) {
            return initializeFuture;
        }
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Directory is fenced"));
        }
        try {
            File dir = new File(path);
            if (!dir.exists()) {
                dir.mkdirs();
            }
            File[] files = dir.listFiles();
            if (ArrayUtils.isEmpty(files)) {
                initializeFuture.complete(null);
                return initializeFuture;
            }
            assert files != null;
            files = Arrays.stream(files).filter(File::isFile).toArray(File[]::new);
            for (var file : files) {
                long fileId = Long.parseLong(file.getName());
                idGenerator.accumulateAndGet(fileId, (oldValue, newValue) -> Math.max(oldValue, newValue + 1));
                var ex = openExistingFile(fileId);
                if (ex != null) {
                    throw ex;
                }
            }
            STATE_UPDATER.set(this, State.INITIALIZED);
            initializeFuture.complete(null);
        } catch (Exception ex) {
            initializeFuture.completeExceptionally(ex);
            STATE_UPDATER.set(this, State.INITIALIZE_FAILED);
        }
        return initializeFuture;
    }


    /**
     * retry 3 times to open an existing file.
     *
     * @return
     */
    private Exception openExistingFile(long id) {
        Exception ex = null;
        for (int i = 0; i < 3; i++) {
            EntryFile file = null;
            try {
                file = new DefaultEntryFile(config, path, id, scheduler);
                file.asyncInitialize().get();
                files.put(id, file);
                return null;
            } catch (Exception e) {
                log.error("Open existing file failed. directory:{}, file: {}, times :{}", path, id, i, e);
                ex = e;
                if (file != null) {
                    file.asyncClose()
                            .thenAccept(__ -> log.info("Close open-failed file finished. directory:{}, file: {}", path, id))
                            .exceptionally(exx -> {
                                log.error("Failed to close open-failed file. directory:{}, file: {}", path, id, exx);
                                return null;
                            });
                }
            }
        }
        return ex;
    }

    @Override
    public CompletableFuture<LongIntPair> asyncAddEntry(ByteBuf buffer) {
        return asyncInitialize()
                .thenCompose(__ -> {
                    var file = lastEntryFile();
                    if (null == file || file.isReadOnly()) {
                        return nextFile().thenCompose(___ -> asyncAddEntry(buffer));
                    }
                    return file.asyncInitialize()
                            .thenCompose(___ -> file.asyncAddEntry(buffer))
                            .thenCompose(offset -> {
                                if (offset < 0) {
                                    log.info("Ready to create new file. Current file {}", file.id());
                                    return nextFile().thenCompose(___ -> asyncAddEntry(buffer));
                                } else {
                                    return CompletableFuture.completedFuture(LongIntPair.create(file.id(), offset));
                                }
                            });
                });

    }

    /**
     * Roll next file.
     *
     * @return a future that can be completed when the next file is created
     */
    private CompletableFuture<Void> nextFile() {
        var id = idGenerator.get();
        return PENDING_CREATE.compute(id, (k, v) -> {
            if (v != null) {
                return v;
            }
            // trigger flush.
            asyncFlush();
            var future = new CompletableFuture<Void>();
            var ex = createNewFile(id);
            if (ex != null) {
                future.completeExceptionally(ex);
            } else {
                future.complete(null);
            }
            return future.thenAccept(__ -> {
                        log.info("Create file finished. directory {}, id {}", path, id);
                        idGenerator.incrementAndGet();
                    })
                    .exceptionally(exx -> {
                        log.info("Create file failed. directory {}, id {}", path, id);
                        return null;
                    });
        });
    }

    /**
     * retry 3 times to create a new file
     *
     * @return
     */
    private Exception createNewFile(long id) {
        Exception ex;
        EntryFile file = null;
        int i = 0;
        do {
            try {
                file = new DefaultEntryFile(config, path, id, scheduler);
                file.asyncInitialize().get();
                files.put(id, file);
                return null;
            } catch (Exception e) {
                log.error("Failed to create new entryFile. directory {}, id {}, times {}", path, id, i, e);
                if (file != null) {
                    try {
                        file.asyncClose().get();
                    } catch (Exception e0) {
                        log.error("Failed to close create-failed file. directory {}, id {}", path, id, e0);
                    }
                }
                ex = e;
                i++;
            }
        } while (i < 3);
        return ex;
    }


    @Override
    public CompletableFuture<ByteBuf> asyncReadEntry(LongIntPair pair) {
        return asyncInitialize()
                .thenCompose(__ -> {
                    var file = files.get(pair.f1);
                    var offset = pair.f2;
                    if (file == null) {
                        return CompletableFuture.failedFuture(new IllegalStateException("File not found: " + pair.f1));
                    }
                    return file.asyncInitialize()
                            .thenCompose(___ -> file.asyncReadEntry(offset));
                });
    }

    @Override
    public synchronized CompletableFuture<Void> asyncClose() {
        return asyncFlush()
                .thenCompose(__ -> {
                    if (!STATE_UPDATER.compareAndSet(this, State.INITIALIZED, State.FENCED)) {
                        return CompletableFuture.completedFuture(null);
                    }
                    var futures = new ArrayList<CompletableFuture<Void>>(files.size());
                    for (var entry : files.entrySet()) {
                        futures.add(entry.getValue().asyncClose());
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenAccept(___ -> log.info("All the files closed, directory {}", path))
                            .exceptionally(ex -> {
                                log.error("Create files failed, directory {}", path, ex);
                                return null;
                            });
                });
    }

    @Override
    public CompletableFuture<Void> asyncDelete() {
        return asyncClose()
                .thenCompose(__ -> {
                    var futures = new ArrayList<CompletableFuture<Void>>(files.size());
                    for (var entry : files.entrySet()) {
                        futures.add(entry.getValue().asyncDelete());
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenAccept(___ -> log.info("All the files deleted, directory {}", path))
                            .exceptionally(ex -> {
                                log.error("Delete files failed, directory {}", path, ex);
                                return null;
                            });
                });
    }

    @Override
    public synchronized CompletableFuture<Void> asyncFlush() {
        if (state.equals(State.FENCED)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Directory is fenced"));
        }
        if (state.equals(State.NEW) || state.equals(State.INITIALIZE_FAILED)) {
            return CompletableFuture.completedFuture(null);
        }
        if (state.equals(State.INITIALIZING)) {
            return initializeFuture.thenCompose(__ -> asyncFlush());
        }

        return initializeFuture
                .thenCompose(__ -> {
                    var futures = new ArrayList<CompletableFuture<Void>>(files.size());
                    for (var entry : files.entrySet()) {
                        futures.add(entry.getValue().asyncFlush());
                    }
                    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                            .thenAccept(___ -> log.info("All the files flushed, directory {}", path))
                            .exceptionally(ex -> {
                                log.error("Flush files failed, directory {}", path, ex);
                                return null;
                            });
                });

    }
}
