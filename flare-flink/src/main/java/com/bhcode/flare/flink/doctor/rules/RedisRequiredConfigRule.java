package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.Redis;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

public class RedisRequiredConfigRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Redis[] annos = jobClass.getAnnotationsByType(Redis.class);
        for (Redis redis : annos) {
            if (isBlank(redis.host())) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-221",
                        "Redis host missing for keyNum=" + redis.keyNum(),
                        "Set @Redis(host=...)"
                ));
            }
            if (redis.port() <= 0) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-222",
                        "Redis port invalid for keyNum=" + redis.keyNum() + ": " + redis.port(),
                        "Set @Redis(port>0)"
                ));
            }
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
