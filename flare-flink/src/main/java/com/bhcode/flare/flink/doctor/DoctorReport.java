package com.bhcode.flare.flink.doctor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public class DoctorReport {

    private final List<Diagnostic> diagnostics = new ArrayList<>();

    public void add(Diagnostic diagnostic) {
        if (diagnostic != null) {
            this.diagnostics.add(diagnostic);
        }
    }

    public List<Diagnostic> diagnostics() {
        return Collections.unmodifiableList(diagnostics);
    }

    public boolean hasErrors() {
        return diagnostics.stream().anyMatch(d -> d.severity() == DiagnosticSeverity.ERROR);
    }

    public String toJson() {
        StringJoiner joiner = new StringJoiner(",", "[", "]");
        for (Diagnostic d : diagnostics) {
            joiner.add("{"
                    + "\"severity\":\"" + escape(String.valueOf(d.severity())) + "\","
                    + "\"code\":\"" + escape(d.code()) + "\","
                    + "\"message\":\"" + escape(d.message()) + "\","
                    + "\"suggestion\":\"" + escape(d.suggestion()) + "\""
                    + "}");
        }
        return joiner.toString();
    }

    private String escape(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
