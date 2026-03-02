package com.bhcode.flare.flink.doctor.rules;

import com.bhcode.flare.common.anno.Config;
import com.bhcode.flare.flink.conf.FlareFlinkConf;
import com.bhcode.flare.flink.doctor.Diagnostic;
import com.bhcode.flare.flink.doctor.DiagnosticSeverity;
import com.bhcode.flare.flink.doctor.DoctorReport;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

public class RuntimeGovernanceGuardrailRule implements DoctorRule {

    private static final String LEGACY_AUTO_START_KEY = "flink.job.autoStart";
    private static final String NORMALIZED_AUTO_START_KEY = FlareFlinkConf.FLINK_JOB_AUTO_START;

    @Override
    public void check(Class<?> jobClass, DoctorReport report) {
        if (jobClass == null || report == null) {
            return;
        }
        Config config = jobClass.getAnnotation(Config.class);
        if (config == null) {
            return;
        }
        Map<String, String> conf = parseConfig(config);
        if (conf.isEmpty()) {
            return;
        }

        if (conf.containsKey(LEGACY_AUTO_START_KEY)) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.WARN,
                    "DR-331",
                    "Deprecated config key detected: " + LEGACY_AUTO_START_KEY,
                    "Use normalized key: " + NORMALIZED_AUTO_START_KEY
            ));
        }

        if (asBoolean(conf.get(FlareFlinkConf.FLARE_RUNTIME_REST_ENABLE))
                && StringUtils.isBlank(conf.get(FlareFlinkConf.FLARE_RUNTIME_REST_TOKEN))) {
            report.add(new Diagnostic(
                    DiagnosticSeverity.WARN,
                    "DR-332",
                    "Runtime REST is enabled without token",
                    "Set " + FlareFlinkConf.FLARE_RUNTIME_REST_TOKEN + " for non-local environments"
            ));
        }

        if (asBoolean(conf.get(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_ENABLE))) {
            int poolSize = parseInt(conf.get(FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_POOL_SIZE), 1);
            if (poolSize <= 0) {
                report.add(new Diagnostic(
                        DiagnosticSeverity.WARN,
                        "DR-333",
                        "Runtime scheduler pool size is invalid: " + poolSize,
                        "Set " + FlareFlinkConf.FLARE_RUNTIME_SCHEDULE_POOL_SIZE + " to a positive integer"
                ));
            }
        }
    }

    private Map<String, String> parseConfig(Config config) {
        Map<String, String> conf = new LinkedHashMap<>();
        if (config.props() != null) {
            for (String item : config.props()) {
                applyKeyValue(conf, item);
            }
        }
        if (StringUtils.isNotBlank(config.value())) {
            String[] parts = config.value().split("[;\\n]");
            for (String part : parts) {
                applyKeyValue(conf, part);
            }
        }
        return conf;
    }

    private void applyKeyValue(Map<String, String> conf, String text) {
        if (StringUtils.isBlank(text)) {
            return;
        }
        String[] kv = text.split("=", 2);
        if (kv.length != 2) {
            return;
        }
        String key = kv[0].trim();
        String value = kv[1].trim();
        if (StringUtils.isBlank(key)) {
            return;
        }
        conf.put(key, value);
    }

    private boolean asBoolean(String value) {
        return "true".equalsIgnoreCase(StringUtils.trimToEmpty(value));
    }

    private int parseInt(String value, int defaultValue) {
        if (StringUtils.isBlank(value)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
