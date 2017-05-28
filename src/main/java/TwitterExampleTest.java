import org.junit.Test;

import java.util.Vector;

import static org.junit.Assert.*;

/**
 * Created by isaac on 5/28/17.
 */
public class TwitterExampleTest {
    @Test
    public void testInitArrayList() throws Exception {
        Vector<String> theList = new Vector<String>();
        theList.add("a");
        theList.add("word");
        TwitterExample a = new TwitterExample();
        assertEquals("theList must be a word",a.initArrayList("test.txt"),theList);

    }

}