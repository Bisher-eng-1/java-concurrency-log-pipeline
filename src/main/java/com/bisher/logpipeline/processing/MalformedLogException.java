package com.bisher.logpipeline.processing;

/**
 * Thrown when a raw log line violates the expected structured log schema.
 */
public class MalformedLogException extends Exception {

    public MalformedLogException(String message) {
        super(message);
    }

    public MalformedLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
