package org.daojun.neutron.storage;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.daojun.neutron.storage.internal.DefaultEntryFile;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

@Test
@SuppressWarnings("unchecked")
public class DefaultEntryFileTest {

    @Test
    public void test() throws Exception {
        int id = 22;
        DefaultEntryFile entryFile = new DefaultEntryFile(null, "/tmp/entry/", id, Executors.newScheduledThreadPool(1));
        entryFile.asyncInitialize().get();


        var offsets = new ArrayList<Integer>();
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(1024);
            for (int j = 0; j < 128; j++) {
                buf.writeLong(i);
            }
            int offset = entryFile.asyncAddEntry(buf).get();
            offsets.add(offset);
            Assert.assertEquals(offset, 12 + i * 1028L);
        }

        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }
        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }
        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }

        entryFile.asyncFlush().get();

        Class<?> clazz = entryFile.getClass();
        Field flushed = clazz.getDeclaredField("flushed");
        Field buffer = clazz.getDeclaredField("buffer");
        flushed.setAccessible(true);
        buffer.setAccessible(true);
        AtomicInteger flushObj = (AtomicInteger) flushed.get(entryFile);
        Assert.assertEquals(flushObj.get(), 1028 * 100 + 12);
        @SuppressWarnings("unchecked")
        Map<Integer, ByteBuf> bufMap = (Map<Integer, ByteBuf>) buffer.get(entryFile);
        Assert.assertEquals(bufMap.size(), 0);

        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
            buf.release();
        }

        entryFile.asyncClose().get();

        DefaultEntryFile entryFile0 = new DefaultEntryFile(null, "/tmp/entry/", id, Executors.newScheduledThreadPool(1));
        ;
        entryFile0.asyncInitialize().get();
        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile0.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }
        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile0.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }
        // read 100 times
        for (int i = 0; i < 100; i++) {
            ByteBuf buf = entryFile0.asyncReadEntry(offsets.get(i)).get();
            for (int j = 0; j < 128; j++) {
                Assert.assertEquals(buf.readLong(), i);
            }
        }
    }

}
