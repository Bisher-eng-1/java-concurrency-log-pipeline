package com.bisher.logpipeline.model;

public enum LogLevel {
    INFO,
    WARN,
    ERROR,
    DEBUG;

    public static LogLevel parseStrict(String token) {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Log level cannot be blank");
        }
        return switch (token.trim()) {
            case "INFO" -> INFO;
            case "WARN" -> WARN;
            case "ERROR" -> ERROR;
            case "DEBUG" -> DEBUG;
            default -> throw new IllegalArgumentException("Unsupported log level: " + token);
        };
    }
}
