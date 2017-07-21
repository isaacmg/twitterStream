import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.AvroRowSerializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Created by isaac on 7/19/17.
 */
public class Kafka010AvroSink extends KafkaTableSink {

    public Kafka010AvroSink(String topic, Properties properties, FlinkKafkaPartitioner<Row> partitioner) {
        		super(topic, properties, partitioner);
        	}
    protected FlinkKafkaProducerBase<Row> createKafkaProducer(String s, Properties properties, SerializationSchema<Row> serializationSchema, FlinkKafkaPartitioner<Row> flinkKafkaPartitioner) {
        return null;
    }

    @Override
    protected SerializationSchema<Row> createSerializationSchema(String[] strings) {

        return null;
        //return new AvroRowSerializationSchema(new AVroSp);
    }

    @Override
    protected KafkaTableSink createCopy() {
        return null;
    }

}
