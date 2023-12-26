package org.daojun.neutron.storage;

public class FencedException extends Exception {
    public FencedException() {
        super();
    }

    public FencedException(String message) {
        super(message);
    }

    public FencedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FencedException(Throwable cause) {
        super(cause);
    }

    protected FencedException(String message, Throwable cause,
                              boolean enableSuppression,
                              boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
