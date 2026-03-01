package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class AutoStartGuardrailRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Streaming streaming = jobClass.getAnnotation(Streaming.class);
        if (streaming == null) {
            return;
        }
        if (!streaming.autoStart()) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.WARN,
                    "DR-321",
                    "Streaming autoStart is disabled",
                    "Keep autoStart=true or document manual env.execute() and deployment steps"
            ));
        }
    }
}
