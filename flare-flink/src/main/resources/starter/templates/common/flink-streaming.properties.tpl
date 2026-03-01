# Flare runtime config for ${JOB_NAME}
# Tune these values for your environment.

flink.appName=${JOB_NAME}
flink.job.autoStart=true
flink.default.parallelism=1
flink.stream.checkpoint.interval=10000

kafka.brokers=localhost:9092
