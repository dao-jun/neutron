package org.daojun.neutron.storage;

public class ManagedStorageConfig {
    private int maxEntryFileSize;
    private int maxEntryCacheOfOneFile;

    public ManagedStorageConfig() {
    }

    public void setMaxEntryFileSize(int maxEntryFileSize) {
        this.maxEntryFileSize = maxEntryFileSize;
    }

    public int getMaxEntryFileSize() {
        return maxEntryFileSize;
    }

    public void setMaxEntryCacheOfOneFile(int maxEntryCacheOfOneFile) {
        this.maxEntryCacheOfOneFile = maxEntryCacheOfOneFile;
    }

    public int getMaxEntryCacheOfOneFile() {
        return maxEntryCacheOfOneFile;
    }
}
