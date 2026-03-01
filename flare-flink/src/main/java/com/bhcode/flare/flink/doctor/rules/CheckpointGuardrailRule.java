package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.anno.Streaming;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class CheckpointGuardrailRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Streaming streaming = jobClass.getAnnotation(Streaming.class);
        if (streaming == null) {
            return;
        }

        int interval = streaming.interval() > 0 ? streaming.interval() : streaming.value();
        if (interval <= 0) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.WARN,
                    "DR-301",
                    "Checkpoint interval is disabled or invalid: " + interval,
                    "Set @Streaming(interval>0) or @Streaming(value>0)"
            ));
        }
    }
}
