package org.daojun.neutron.storage;

public class RollFileException extends Exception {
    public RollFileException(String message) {
        super(message);
    }

    public RollFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public RollFileException(Throwable cause) {
        super(cause);
    }
}
