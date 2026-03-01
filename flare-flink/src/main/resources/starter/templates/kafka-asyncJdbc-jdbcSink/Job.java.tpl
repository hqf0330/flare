package ${PACKAGE};

import com.bhcode.flare.core.anno.connector.Jdbc;
import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import org.apache.flink.streaming.api.datastream.DataStream;

@Streaming(parallelism = 1, interval = 10)
@Kafka(brokers = "localhost:9092", topics = "input_topic", groupId = "${JOB_NAME}_group")
@Jdbc(
        url = "jdbc:mysql://localhost:3306/demo",
        username = "root",
        password = "password"
)
public class ${JOB_NAME} extends FlinkStreaming {

    public record SinkRow(String rawData) {
    }

    @Override
    public void process() {
        DataStream<String> source = this.kafkaSourceFromConf();
        DataStream<SinkRow> rows = source.map(SinkRow::new);
        this.jdbcSink(rows, "t_demo_sink");
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(${JOB_NAME}.class, args);
    }
}
