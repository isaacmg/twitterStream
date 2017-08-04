/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;

// REPLACE KafkaJSONTABLESINK with new one 
public class Kafka010JsonTableSink extends KafkaJsonTableSink {


    public Kafka010JsonTableSink(String topic, Properties properties, FlinkKafkaPartitioner<Row> partitioner) {
        super(topic, properties, partitioner);
    }

    /**
     * Creates for Kafka 0.9 .
     *
     * @param topic       topic in Kafka to which table is written
     * @param properties  properties to connect to Kafka
     * @param partitioner Kafka partitioner
     * @deprecated This is a deprecated constructor that does not correctly handle partitioning when
     * producing to multiple topics. Use
     * {@link #Kafka010JsonTableSink(String, Properties, FlinkKafkaPartitioner)} instead.
     */
    @Deprecated

    @Override
    protected FlinkKafkaProducerBase<Row> createKafkaProducer(String topic, Properties properties, SerializationSchema<Row> serializationSchema, FlinkKafkaPartitioner<Row> partitioner) {
        return new FlinkKafkaProducer010<>(topic, serializationSchema, properties, partitioner);
    }

    @Override
    protected Kafka010JsonTableSink createCopy() {
        return new Kafka010JsonTableSink(topic, properties, partitioner);
    }
}