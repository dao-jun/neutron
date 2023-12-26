package org.daojun.neutron.storage;

public class LongIntPair {
    public final long f1;
    public final int f2;

    private LongIntPair(long f1, int f2) {
        this.f1 = f1;
        this.f2 = f2;
    }


    public static LongIntPair create(long f1, int f2) {
        return new LongIntPair(f1, f2);
    }

    @Override
    public String toString() {
        return "LongIntPair{" +
                "f1=" + f1 +
                ", f2=" + f2 +
                '}';
    }
}
