package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public interface LedgerHandle {

    long getId();

    CompletableFuture<Position> asyncAddEntry(ByteBuf buffer);

    CompletableFuture<ByteBuf> asyncReadEntry(Position position);

    void setLac(long entryId);

    long getLac();

}
