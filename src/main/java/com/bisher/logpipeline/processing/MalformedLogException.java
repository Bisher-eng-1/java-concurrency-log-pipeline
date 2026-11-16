package com.bisher.logpipeline.processing;

public class MalformedLogException extends Exception {

    public MalformedLogException(String message) {
        super(message);
    }

    public MalformedLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
