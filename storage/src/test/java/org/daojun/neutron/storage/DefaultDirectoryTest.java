package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.daojun.neutron.storage.internal.DefaultDirectory;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

@Test
public class DefaultDirectoryTest {
    @Test
    public void test() throws Exception {
        var config = new ManagedStorageConfig();
        config.setMaxEntryFileSize(1024 * 1024 * 1024);
        config.setMaxEntryCacheOfOneFile(512 * 1024 * 1024);
        var directory = new DefaultDirectory(config, "/tmp/d6", new AtomicLong(0), Executors.newScheduledThreadPool(1));
        directory.asyncInitialize().get();

        var latch = new CountDownLatch(10);

//        List<LongIntPair> positions = Collections.synchronizedList(new ArrayList<>());

        long start = System.currentTimeMillis();
        for (int a = 0; a < 100; a++) {
            new Thread(() -> {
                for (int i = 0; i < 102400; i++) {
                    ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(1024);
                    for (int j = 0; j < 128; j++) {
                        buf.writeLong(i);
                    }
                    try {
                        var pos = directory.asyncAddEntry(buf).get();
//                        positions.add(pos);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                    buf.release();
                }
                latch.countDown();
            }).start();
        }

        latch.await();
        System.out.println("time: " + (System.currentTimeMillis() - start));
        directory.asyncFlush();

//        for (var pos : positions) {
//            var buf = directory.asyncReadEntry(pos).get();
//            Assert.assertEquals(buf.readableBytes(), 1024);
//            buf.release();
//        }

        System.out.println();
    }
}
