package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.core.anno.connector.HBase;
import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.core.anno.connector.Redis;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

public class ConnectorAnnotationKeyRule implements DoctorRule {

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        checkDupKeyNum("Kafka", jobClass.getAnnotationsByType(Kafka.class), report);
        checkDupKeyNum("Jdbc", jobClass.getAnnotationsByType(Jdbc.class), report);
        checkDupKeyNum("Redis", jobClass.getAnnotationsByType(Redis.class), report);
        checkDupKeyNum("HBase", jobClass.getAnnotationsByType(HBase.class), report);
    }

    private void checkDupKeyNum(String family, Annotation[] annotations, DoctorReport report) {
        if (annotations == null || annotations.length <= 1) {
            return;
        }
        Set<Integer> seen = new HashSet<>();
        for (Annotation annotation : annotations) {
            int keyNum = keyNum(annotation);
            if (!seen.add(keyNum)) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.ERROR,
                        "DR-011",
                        family + " annotation has duplicate keyNum=" + keyNum,
                        "Use unique keyNum for each " + family + " annotation"
                ));
                return;
            }
        }
    }

    private int keyNum(Annotation annotation) {
        if (annotation == null) {
            return 1;
        }
        try {
            Method method = annotation.annotationType().getMethod("keyNum");
            Object value = method.invoke(annotation);
            if (value instanceof Number number) {
                return number.intValue();
            }
            return Integer.parseInt(String.valueOf(value));
        } catch (Exception e) {
            return 1;
        }
    }
}
