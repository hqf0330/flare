package ${PACKAGE};

import com.bhcode.flare.core.anno.connector.Kafka;
import com.bhcode.flare.flink.FlinkJobLauncher;
import com.bhcode.flare.flink.FlinkStreaming;
import com.bhcode.flare.flink.anno.Streaming;
import org.apache.flink.streaming.api.datastream.DataStream;

@Streaming(parallelism = 1, interval = 10)
@Kafka(brokers = "localhost:9092", topics = "input_topic", groupId = "${JOB_NAME}_group")
public class ${JOB_NAME} extends FlinkStreaming {

    @Override
    public void process() {
        DataStream<String> source = this.kafkaSourceFromConf();
        source.map(String::trim).print();
    }

    public static void main(String[] args) {
        FlinkJobLauncher.run(${JOB_NAME}.class, args);
    }
}
