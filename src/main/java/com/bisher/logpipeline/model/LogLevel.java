package com.bisher.logpipeline.model;

/**
 * Supported log event severity levels.
 */
public enum LogLevel {
    INFO,
    WARN,
    ERROR,
    DEBUG;

    /**
     * Parses a severity token case-sensitively against canonical log levels.
     *
     * @param token raw level string from log line
     * @return matching {@link LogLevel}
     * @throws IllegalArgumentException if token is null or unrecognized
     */
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
