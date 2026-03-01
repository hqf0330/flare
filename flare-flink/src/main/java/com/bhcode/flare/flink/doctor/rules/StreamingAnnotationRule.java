package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class StreamingAnnotationRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        if (!jobClass.isAnnotationPresent(Streaming.class)) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.ERROR,
                    "DR-001",
                    "Missing @Streaming annotation",
                    "Add @Streaming to the job class"
            ));
        }
    }
}
