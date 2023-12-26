package org.daojun.neutron.storage.internal;

import io.netty.util.Recycler;
import org.daojun.neutron.common.utils.NumberUtils;

class LongIntBytes {
    final byte[] memory = new byte[12];

    void set(long f1, int f2) {
        NumberUtils.putLong(memory, 0, f1);
        NumberUtils.putInt(memory, 8, f2);
    }

    long f1() {
        return NumberUtils.readLong(memory, 0);
    }

    int f2() {
        return NumberUtils.readInt(memory, 8);
    }

    static LongIntBytes get(long f1, int f2) {
        LongIntBytes lp = RECYCLER.get();
        NumberUtils.putLong(lp.memory, 0, f1);
        NumberUtils.putInt(lp.memory, 8, f2);
        return lp;
    }

    static LongIntBytes get() {
        return RECYCLER.get();
    }

    void recycle() {
        handle.recycle(this);
    }

    private static final Recycler<LongIntBytes> RECYCLER = new Recycler<>() {
        @Override
        protected LongIntBytes newObject(Handle<LongIntBytes> handle) {
            return new LongIntBytes(handle);
        }
    };

    private final Recycler.Handle<LongIntBytes> handle;

    private LongIntBytes(Recycler.Handle<LongIntBytes> handle) {
        this.handle = handle;
    }
}
