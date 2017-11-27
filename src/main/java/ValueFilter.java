import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;

/**
 * Created by isaac on 11/26/17.
 */
abstract class ValueFilter extends SimpleCondition {
    protected int value;
    ValueFilter(int val) {
        value = val;
    }

}
//Solve stupid object type problem even though nothing wrong
class ValueTupleFilter extends ValueFilter{

    public boolean filter(Tuple2<String,Integer> subEvent) {
        int i = subEvent.getField(1);
        return i > value;
    }

}

