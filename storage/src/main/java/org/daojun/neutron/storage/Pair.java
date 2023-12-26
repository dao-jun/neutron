package org.daojun.neutron.storage;

public class Pair<F1, F2> {
    public final F1 f1;
    public final F2 f2;

    private Pair(F1 f1, F2 f2) {
        this.f1 = f1;
        this.f2 = f2;
    }

    public static <F1, F2> Pair<F1, F2> create(F1 f1, F2 f2) {
        return new Pair<>(f1, f2);
    }
}
