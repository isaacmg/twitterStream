import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by isaac on 5/28/17.
 */
public class TwitterExampleTest {
    TwitterExample a = new TwitterExample();
    TwitterExample.JSONIZEString j = new TwitterExample.JSONIZEString();
    @Test
    public void testInitArrayList() throws Exception {
        Vector<String> theList = new Vector<String>();
        theList.add("a");
        theList.add("word");
        TwitterExample a = new TwitterExample();
        assertEquals("theList must be a word",a.initArrayList("test.txt"),theList);

    }
    @Test
    public  void testEqualize(){
        TwitterExample a = new TwitterExample();
        String one = a.tokenize("test23123...");

        assertEquals("string two should be whatwe", a.tokenize("whatwe"), "whatwe");

    }
    @Test
    public void testJSONIZEString(){


    }

}