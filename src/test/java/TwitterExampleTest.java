import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by isaac on 5/28/17.
 */
public class TwitterExampleTest {
    TwitterExample a = new TwitterExample();
    @Test
    public void testInitArrayList() throws Exception {
        Vector<String> theList = new Vector<String>();
        theList.add("a");
        theList.add("word");
        //assertEquals("theList must be a word",a.initArrayList("test.txt", new ClassLoader() {
        //}),theList);

    }
    @Test
    public  void testEqualize(){

        String one = a.tokenize("test23123...");

        assertEquals("string two should be whatwe", a.tokenize("whatwe"), "whatwe");

    }
    @Test
    public void testFlinkInit(){
        //TODO WRITE PROPER COMPARATOR METHOD FOR TEST

//        FlinkKafkaProducer09<String> prod = a.initKafkaProducer("localhost:9090","test");
//
//        FlinkKafkaProducer09<String> myProducer = new FlinkKafkaProducer09<String>(
//                "localhost:9090",            // broker list
//                "test",                  // target topic
//                new SimpleStringSchema());   // serialization schema
//
//        // the following is necessary for at-least-once delivery guarantee
//        myProducer.setLogFailuresOnly(false);   // "false" by default
//        myProducer.setFlushOnCheckpoint(true);
//        assertEquals("kafka producer should be equal", prod,myProducer);

    }

}