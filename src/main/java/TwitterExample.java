/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.QueryableStateStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.connectors.kafka.*;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import javax.json.Json;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;


/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON objects in a streaming fashion.
 * <p>
 * The input is a Tweet stream from a TwitterSource.
 * </p>
 * <p>
 * Usage: <code>Usage: TwitterExample [--output <path>]
 * [--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]</code><br>
 *
 * If no parameters are provided, the program is run with default data from
 * {@link TwitterExampleData}.
 * </p>
 * <p>
 * This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */
public class TwitterExample {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************
    public static Vector<String> initArrayList(String path, ClassLoader cl) throws FileNotFoundException, UnsupportedEncodingException{

        InputStream is = cl.getResourceAsStream(path);

        BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        Vector<String> wordStops = new Vector<>();
        try {
            String line = br.readLine();


            while (line != null) {

                wordStops.add(line);
                line = br.readLine();
            }
            br.close();
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return wordStops;

    }

    public static String tokenize(String sentence) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < sentence.length(); i++) {
            char c = sentence.charAt(i);

            if (Character.isLetter(c)) {
                s.append(c);
            } else if (c == ' ' || c == '\t' || c == '\r') {
                s.append(' ');
            }
            else {
                s.append(' ');
            }
        }
        return s.toString();

    }
    public static FlinkKafkaProducer010 initKafkaProducer(String host, String topic){
        FlinkKafkaProducer010<String> myProducer = new FlinkKafkaProducer010<String>(
                host,            // broker list
                topic,                  // target topic
                new SimpleStringSchema());   // serialization schema

        // the following is necessary for at-least-once delivery guarantee
        myProducer.setLogFailuresOnly(false);   // "false" by default
        myProducer.setFlushOnCheckpoint(true);  // "false" by default
        return myProducer;

    }
    private static Kafka010JSONTableSink makeTableSink(String theTopic, Properties myProperties){

        FlinkKafkaPartitioner<Row> row2 = new FlinkFixedPartitioner<>();

        return new Kafka010JSONTableSink(theTopic,myProperties, row2);

    }


    public static void main(String[] args) throws Exception {

        //Use  class loader to load the file
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream("myFile.properties");
        // copy config from Java resource to a file
        File configOnDisk = new File("myFile.properties");
        Files.copy(classloader.getResourceAsStream("myFile.properties"), configOnDisk.toPath(), StandardCopyOption.REPLACE_EXISTING);
        final ParameterTool params = ParameterTool.fromPropertiesFile("myFile.properties");
        System.out.println("Usage: TwitterExample [--output <path>] " +
                "[--twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> --twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret>]");

        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setParallelism(params.getInt("parallelism", 1));
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //DataStream<String> streamSource = env.addSource(new TwitterSource("/myFile.properties"));
        System.out.println(" This is the param" + params.getProperties());

        // get input data
        DataStream<String> streamSource;

        if (params.has(TwitterSource.CONSUMER_KEY) &&
                params.has(TwitterSource.CONSUMER_SECRET) &&
                params.has(TwitterSource.TOKEN) &&
                params.has(TwitterSource.TOKEN_SECRET)
                ) {


            final Vector<String> theList = initArrayList("words.txt", classloader);

            //Find tweets about Trump and Clinton
            TwitterSource twitterA = new TwitterSource(params.getProperties());
            TwitterSourceOpt.FilterEndpoint i = new TwitterSourceOpt.FilterEndpoint(theList);
            twitterA.setCustomEndpointInitializer(i);

            streamSource = env.addSource(twitterA);

        } else {
            System.out.println("Executing TwitterStream example with default props.");
            System.out.println("Use --twitter-source.consumerKey <key> --twitter-source.consumerSecret <secret> " +
                    "--twitter-source.token <token> --twitter-source.tokenSecret <tokenSecret> specify the authentication info.");
            // get default test text data
            streamSource = env.fromElements(TwitterExampleData.TEXTS);
        }
        final Vector<String> stopWords = initArrayList("stopwords.txt", classloader);


        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new SelectEnglishAndTokenizeFlatMap("text"));

        //Get locations
        DataStream<Tuple2<String, Integer>> locations = streamSource.flatMap(new SelectEnglishAndTokenizeFlatMap("location")).keyBy(0).sum(1);
        tweets.keyBy(0).asQueryableState("Twitter tweets by key");
        //Filter out stop words
        tweets = tweets.filter(new FilterFunction<Tuple2<String, Integer>>(){
            public boolean filter(Tuple2<String,Integer> value){
                String word = value.getField(0);
                return !stopWords.contains(word);

            }
        });

        DataStream<Tuple2<String,Integer>> dataWindowKafka = tweets.keyBy(0).timeWindow(Time.seconds(10)).sum(1).filter(new FilterFunction<Tuple2<String, Integer>>() {
            public boolean filter(Tuple2<String, Integer> value) { int s = value.getField(1); return s > 10; }
        });


        dataWindowKafka.map(new JSONIZEString());
        Pattern<Tuple2<String, Integer>, ?> pattern  =  Pattern.<Tuple2<String,Integer>>begin("first").where(new SimpleCondition2(15)).followedBy("increasing").where(new SimpleCondition2(2));
        PatternStream<Tuple2<String, Integer>> patternStream = CEP.pattern(dataWindowKafka, pattern);
        DataStream<String> manyMentions = patternStream.select(new PatternSelectFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String select(Map<String, List<Tuple2<String, Integer>>> map) throws Exception {
                return "the word " + map.toString() ;
            }
        });


                          //Temporarily disabled Kafka for testing purposes uncomment the following to re-enable
                          //Initialize a Kafka producer that will be consumed by D3.js and (possibly the database).
                          //FlinkKafkaProducer010 myProducer = initKafkaProducer("localhost:9092","test");
                          //dataWindowKafka.map(new JSONIZEString()).addSink(myProducer);

                          //Transition to a table environment

                          StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
                          // tableEnv.registerDataStream("myTable2", dataWindowKafka, "word, count");
                          Table table2 = tableEnv.fromDataStream(dataWindowKafka, "word, count");
                          // Confusing
                          //System.out.println("This is the table name " + table2.where("count>5"));
                          // Using a CSV TableSink
                          //TableSink sink = new CsvTableSink("path54.csv", ",");
                          //table2.writeToSink(sink);
                          Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers","localhost:9092");
        kafkaProperties.setProperty("group.id", "test");
        kafkaProperties.setProperty("zookeeper.connect","localhost:2181");
                          KafkaTableSink10 plotSink =  makeTableSink("twitter",kafkaProperties);
                          //table2.writeToSink(plotSink);

        env.execute("Twitter Streaming Example");

                      }


                // *************************************************************************
                // USER FUNCTIONS
                // *************************************************************************

                /**
                 * Deserialize JSON from twitter source
                 *
                 * <p>
                 * Implements a string tokenizer that splits sentences into words as a
                 * user-defined FlatMapFunction. The function takes a line (String) and
                 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
                 * Integer>}).
                 */
        public static class JSONIZEString implements MapFunction<Tuple2<String, Integer>, String> {


            public String map(Tuple2<String, Integer> in) {

                String jsonString = Json.createObjectBuilder()
                        .add("word", in.f0)
                        .add("count", in.f1)
                        .add("time", System.currentTimeMillis())
                        .build()
                        .toString();

                System.out.println(jsonString);

                return jsonString;
            }
        }


        public static class SelectEnglishAndTokenizeFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
            private static final long serialVersionUID = 1L;
            private String fieldName;
            private transient ObjectMapper jsonParser;
            SelectEnglishAndTokenizeFlatMap(String fieldName1){
                fieldName=fieldName1;
            }
            /**
             * Select the language from the incoming JSON text
             */
            private StringTokenizer getField(JsonNode jsonNode){
                if(fieldName.equals("text")&& jsonNode.has(fieldName)){
                    return new StringTokenizer(jsonNode.get(fieldName).getValueAsText());
                }

                return new StringTokenizer(jsonNode.get("user").get(fieldName).getValueAsText());

            }
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                if(jsonParser == null) {
                    jsonParser = new ObjectMapper();
                }
                JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);

                boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").getValueAsText().equals("en");

                if (isEnglish) {

                    StringTokenizer tokenizer = getField(jsonNode);
                    // split the message
                    while (tokenizer.hasMoreTokens()) {
                        String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
                        result = tokenize(result);
                        if (!result.equals("")) {
                            out.collect(new Tuple2<>(result, 1));
                        }
                    }
                }
            }

        }

    }
