package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class KafkaRequiredConfigRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Kafka[] annos = jobClass.getAnnotationsByType(Kafka.class);
        for (Kafka kafka : annos) {
            if (isBlank(kafka.brokers())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-101",
                        "Kafka brokers missing for keyNum=" + kafka.keyNum(),
                        "Set @Kafka(brokers=...)"
                ));
            }
            if (isBlank(kafka.topics())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-102",
                        "Kafka topics missing for keyNum=" + kafka.keyNum(),
                        "Set @Kafka(topics=...)"
                ));
            }
            if (isBlank(kafka.groupId())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-103",
                        "Kafka groupId missing for keyNum=" + kafka.keyNum(),
                        "Set @Kafka(groupId=...)"
                ));
            }
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
