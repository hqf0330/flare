package com.bhcode.flare.flink.doctor;

public record Diagnostic(
        DiagnosticSeverity severity,
        String code,
        String message,
        String suggestion
) {
}
