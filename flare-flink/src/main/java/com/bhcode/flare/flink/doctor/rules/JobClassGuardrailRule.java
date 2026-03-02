package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

import java.lang.reflect.Modifier;

public class JobClassGuardrailRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        if (!FlinkStreaming.class.isAssignableFrom(jobClass)) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.ERROR,
                    "DR-041",
                    "Job class must extend FlinkStreaming",
                    "Make job class extend com.bhcode.flare.flink.FlinkStreaming"
            ));
            return;
        }

        if (Modifier.isAbstract(jobClass.getModifiers())) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.ERROR,
                    "DR-042",
                    "Job class must be concrete (not abstract)",
                    "Use a concrete class and implement process()"
            ));
        }
    }
}
