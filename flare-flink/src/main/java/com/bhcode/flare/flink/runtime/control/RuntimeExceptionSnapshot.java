package com.bhcode.flare.flink.runtime.control;

public record RuntimeExceptionSnapshot(
        String exceptionClass,
        String message,
        String stackTrace,
        long timestamp
) {
}
