package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class JdbcRequiredConfigRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Jdbc[] annos = jobClass.getAnnotationsByType(Jdbc.class);
        for (Jdbc jdbc : annos) {
            if (isBlank(jdbc.url())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-201",
                        "JDBC url missing for keyNum=" + jdbc.keyNum(),
                        "Set @Jdbc(url=...)"
                ));
            }
            if (isBlank(jdbc.username())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-202",
                        "JDBC username missing for keyNum=" + jdbc.keyNum(),
                        "Set @Jdbc(username=...)"
                ));
            }
            if (isBlank(jdbc.password())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.WARN,
                        "DR-203",
                        "JDBC password empty for keyNum=" + jdbc.keyNum(),
                        "Set @Jdbc(password=...) or ensure password-less auth is intended"
                ));
            }
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
