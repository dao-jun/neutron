package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public interface Directory {
    /**
     * Total size of the directory.
     *
     * @return size in bytes
     */
    long used();

    /**
     * Total size allowed for the directory.
     *
     * @return size in bytes
     */
    long allowed();

    /**
     * Path of the directory.
     *
     * @return path
     */
    String path();

    /**
     * Whether the directory contains the file.
     *
     * @param fileId
     * @return true if the directory contains the file
     */
    boolean containsFile(long fileId);

    /**
     * Last entry file.
     *
     * @return last entry file
     */
    EntryFile lastEntryFile();

    /**
     * Entry files.
     *
     * @return entry files
     */
    Collection<EntryFile> entryFiles();

    /**
     * Initialize the directory.
     *
     * @return a future that is completed when the directory is initialized
     */
    CompletableFuture<Void> asyncInitialize();

    /**
     * Add an entry to the directory.
     *
     * @param buffer entry
     * @return fileId, offset
     */
    CompletableFuture<LongIntPair> asyncAddEntry(ByteBuf buffer);

    /**
     * Read an entry from the directory.
     *
     * @param pair fileId, offset
     * @return entry
     */
    CompletableFuture<ByteBuf> asyncReadEntry(LongIntPair pair);

    /**
     * Close the directory.
     *
     * @return a future that is completed when the directory is closed
     */
    CompletableFuture<Void> asyncClose();

    /**
     * Delete the directory.
     *
     * @return a future that is completed when the directory is deleted
     */
    CompletableFuture<Void> asyncDelete();

    /**
     * Flush the directory.
     *
     * @return
     */
    CompletableFuture<Void> asyncFlush();
}
