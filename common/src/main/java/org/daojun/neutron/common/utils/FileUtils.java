package org.daojun.neutron.common.utils;

import java.io.File;

public class FileUtils {
    private FileUtils() {
    }

    public static void mkdirs(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    public static String getFileName(String directory, String path) {
        if (directory.endsWith("/")) {
            return directory + path;
        } else {
            return directory + "/" + path;
        }
    }
}
