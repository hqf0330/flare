package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.flink.doctor.DoctorReport;

public interface DoctorRule {

    void check(Class<?> jobClass, DoctorReport report);
}
