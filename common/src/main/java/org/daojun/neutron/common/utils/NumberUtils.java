package org.daojun.neutron.common.utils;

public class NumberUtils {
    private NumberUtils() {
    }

    public static int readInt(byte[] bytes, int offset) {
        return (bytes[offset] & 0xFF) << 24 |
                (bytes[offset + 1] & 0xFF) << 16
                | (bytes[offset + 2] & 0xFF) << 8 |
                bytes[offset + 3] & 0xFF;
    }


    public static long readLong(byte[] bytes, int offset) {
        return (long) (bytes[offset] & 0xFF) << 56
                | (long) (bytes[offset + 1] & 0xFF) << 48
                | (long) (bytes[offset + 2] & 0xFF) << 40
                | (long) (bytes[offset + 3] & 0xFF) << 32
                | (long) (bytes[offset + 4] & 0xFF) << 24
                | (long) (bytes[offset + 5] & 0xFF) << 16
                | (long) (bytes[offset + 6] & 0xFF) << 8
                | (long) (bytes[offset + 7] & 0xFF);
    }


    public static void putInt(byte[] bytes, int offset, int value) {
        bytes[offset] = (byte) (value >>> 24);
        bytes[offset + 1] = (byte) (value >>> 16);
        bytes[offset + 2] = (byte) (value >>> 8);
        bytes[offset + 3] = (byte) value;
    }

    public static void putLong(byte[] bytes, int offset, long value) {
        bytes[offset] = (byte) (value >>> 56);
        bytes[offset + 1] = (byte) (value >>> 48);
        bytes[offset + 2] = (byte) (value >>> 40);
        bytes[offset + 3] = (byte) (value >>> 32);
        bytes[offset + 4] = (byte) (value >>> 24);
        bytes[offset + 5] = (byte) (value >>> 16);
        bytes[offset + 6] = (byte) (value >>> 8);
        bytes[offset + 7] = (byte) value;
    }
}
