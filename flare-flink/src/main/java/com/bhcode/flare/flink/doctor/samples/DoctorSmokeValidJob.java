package com.bhcode.flare.flink.doctor.samples;

import com.bhcode.flare.flink.anno.Streaming;

@Streaming(parallelism = 1, interval = 10)
public class DoctorSmokeValidJob {
}
