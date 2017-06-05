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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.TimestampAssigner;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import javax.json.Json;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.Vector;


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
    private static String path;

    // *************************************************************************
    // PROGRAM
    // *************************************************************************
    public static Vector<String> initArrayList(String path) throws FileNotFoundException{
        TwitterExample.path = path;
        BufferedReader br = new BufferedReader(new FileReader(path));
        Vector<String> wordStops = new Vector<String>();
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
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < sentence.length(); i++) {
            char c = sentence.charAt(i);

            if (Character.isLetter(c)) {
                s.append(c);
            } else if (c == ' ' || c == '\t' || c == '\r') {
                s.append(' ');
            }
        }
        return s.toString();
    }
    public static FlinkKafkaProducer09 initKafkaProducer(String host, String topic){
        FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(
                host,            // broker list
                topic,                  // target topic
                new SimpleStringSchema());   // serialization schema

       // the following is necessary for at-least-once delivery guarantee
        myProducer.setLogFailuresOnly(false);   // "false" by default
        myProducer.setFlushOnCheckpoint(true);  // "false" by default
        return myProducer;

    }

    public static void main(String[] args) throws Exception {
        
        // Checking input parameters
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

            //streamSource = env.addSource(new TwitterSource(params.getProperties()));
            Vector<String> theList = initArrayList("words.txt");

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
        final Vector<String> stopWords = initArrayList("stopwords.txt");

        DataStream<Tuple2<String, Integer>> tweets = streamSource
                // selecting English tweets and splitting to (word, 1)
                .flatMap(new SelectEnglishAndTokenizeFlatMap("text"));

        //Get locations
        DataStream<Tuple2<String, Integer>> locations = streamSource.flatMap(new SelectEnglishAndTokenizeFlatMap("location")).keyBy(0).sum(1);
        //Filter out stop words
        tweets = tweets.filter(new FilterFunction<Tuple2<String, Integer>>(){
            public boolean filter(Tuple2<String,Integer> value){
                String word = value.getField(0);
                return !stopWords.contains(word);

            }
        });
        //
        DataStream<Tuple2<String,Integer>> dataWindowKafka = tweets.keyBy(0).timeWindow(Time.seconds(10)).sum(1).filter(new FilterFunction<Tuple2<String, Integer>>() {
            public boolean filter(Tuple2<String, Integer> value) { int s = value.getField(1); return s > 11; }
        });


        dataWindowKafka.map(new JSONIZEString());







        // emit result
        if (params.has("output")) {
            tweets.writeAsText(params.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            //tweets.print();
            //locations.print();
            dataWindowKafka.print();
        }

        //Initialize a Kafka producer that will be consumed by D3.js and DB.
        FlinkKafkaProducer09 myProducer = initKafkaProducer("localhost:9090","test");
        dataWindowKafka.map(new JSONIZEString()).addSink(myProducer);



        // execute program
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
                        out.collect(new Tuple2<String,Integer>(result, 1));
                    }
                }
            }
        }

    }



}
