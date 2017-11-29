import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

import sun.java2d.pipe.SpanShapeRenderer;

/**
 * Created by isaac on 11/26/17.
 */




class SimpleCondition2<String> extends SimpleCondition<String> {

    SimpleCondition2(int val){

    }

    public boolean filter(String value) throws Exception {
        return false;
    }
}



