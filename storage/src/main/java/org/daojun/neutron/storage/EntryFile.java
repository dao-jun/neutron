package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public interface EntryFile {
    /**
     * Get the id of the entry file
     *
     * @return the id of the entry file
     */
    long id();

    /**
     * Get the path of the entry file
     *
     * @return the path of the entry file
     */
    String path();

    /**
     * Get the size of the entry file
     *
     * @return the size of the entry file
     */
    int size();

    /**
     * Is the entry file read only
     *
     * @return true if the entry file is read only
     */
    boolean isReadOnly();

    /**
     * Initialize the FileChannel and recover metadata, and create a new entry file if not exist.
     * Must be called before any other read/write method.
     *
     * @return a future that can be completed when the entry file is ready to use
     * @throws java.io.IOException   if the entry file cannot be created or opened
     * @throws IllegalStateException if magic number is not matched
     * @throws IllegalStateException if write position is not equal to flush position
     */
    CompletableFuture<Void> asyncInitialize();

    /**
     * Flush the entry file to make sure all the data is persisted
     *
     * @return a future that can be completed when the entry file is flushed
     */
    CompletableFuture<Void> asyncFlush();

    /**
     * Close the entry file
     *
     * @return a future that can be completed when the entry file is closed
     * @throws java.io.IOException if the entry file cannot be closed
     */
    CompletableFuture<Void> asyncClose();

    /**
     * Get the write position of the entry file
     *
     * @return the write position of the entry file
     * @throws FencedException     if the entry file is already closed
     * @throws java.io.IOException if the entry file cannot be created or opened
     */
    CompletableFuture<ByteBuf> asyncReadEntry(int offset);

    /**
     * Add entry to the entry file
     *
     * @param buffer the entry to add
     * @return the offset of the entry, or -1 if the entry file is larger than the maximum size allowed
     * @throws FencedException if the entry file is already closed
     */
    CompletableFuture<Integer> asyncAddEntry(ByteBuf buffer);

    CompletableFuture<Void> asyncDelete();

}
