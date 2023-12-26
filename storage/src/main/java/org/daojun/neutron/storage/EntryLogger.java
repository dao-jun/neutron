package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;

import java.util.concurrent.CompletableFuture;

public interface EntryLogger {

    /**
     * Open a new ledger.
     * @return
     */
    CompletableFuture<LedgerHandle> asyncOpenLedger();

    /**
     * Open an existing ledger.
     *
     * @param ledgerId
     * @return
     */
    CompletableFuture<LedgerHandle> asyncOpenLedger(long ledgerId);

    CompletableFuture<Void> asyncCloseLedger(long ledgerId);

    CompletableFuture<Void> asyncDeleteLedger(long ledgerId);

    CompletableFuture<ByteBuf> asyncReadEntry(Position position);

    CompletableFuture<Void> asyncAddEntry(Position position, ByteBuf buf);
}
