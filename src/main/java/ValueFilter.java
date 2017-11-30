import org.apache.flink.api.common.functions.FilterFunction;

import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.api.java.tuple.Tuple2;
/**
 * Created by isaac on 11/26/17.
 */




class SimpleCondition2 extends SimpleCondition<Tuple2<String,Integer>> {
    public int theValue;
    SimpleCondition2(int val){
        theValue = val;

    }

    public boolean filter(Tuple2<String,Integer> value) throws Exception {
        Tuple2<String, Integer> s = value;
        int i = s.getField(1);

        return i > theValue;
    }
}



