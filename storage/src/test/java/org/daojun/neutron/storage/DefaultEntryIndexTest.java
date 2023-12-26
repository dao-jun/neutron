package org.daojun.neutron.storage;

import org.daojun.neutron.storage.internal.DefaultEntryIndex;
import org.daojun.neutron.storage.internal.DefaultPosition;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DefaultEntryIndexTest {

    @Test
    public void test() throws Exception {
        DefaultEntryIndex entryIndex = null;
        try {
            entryIndex = new DefaultEntryIndex(null, "/tmp");

            for (int i = 0; i < 100; i++) {
                entryIndex.asyncAdd(DefaultPosition.create(100L, i), LongIntPair.create(100L, i));
            }

            for (int i = 0; i < 100; i++) {
                var value = entryIndex.asyncGet(DefaultPosition.create(100L, i)).get();
                Assert.assertNotNull(value);
                Assert.assertEquals(value.f1, 100L);
                Assert.assertEquals(value.f2, i);
            }

            entryIndex.asyncDelete(100L).get();

            for (int i = 0; i < 100; i++) {
                var value = entryIndex.asyncGet(DefaultPosition.create(100L, i)).get();
                Assert.assertNull(value);
            }
            entryIndex.asyncFlush().get();
        } finally {
            if (entryIndex != null) {
                entryIndex.asyncClose().get();
            }
        }

    }

}
