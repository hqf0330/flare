package com.bhcode.flare.flink.doctor;

import com.bhcode.flare.flink.doctor.rules.DoctorRule;

import java.util.Collections;
import java.util.List;

public class DoctorRunner {

    private final List<DoctorRule> rules;

    public DoctorRunner(List<DoctorRule> rules) {
        this.rules = rules == null ? List.of() : List.copyOf(rules);
    }

    public List<DoctorRule> rules() {
        return Collections.unmodifiableList(rules);
    }

    public DoctorReport run(Class<?> jobClass) {
        DoctorReport report = new DoctorReport();
        if (jobClass == null) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.ERROR,
                    "DR-000",
                    "jobClass is null",
                    "Pass a valid job class"
            ));
            return report;
        }
        for (DoctorRule rule : rules) {
            if (rule != null) {
                rule.check(jobClass, report);
            }
        }
        return report;
    }
}
