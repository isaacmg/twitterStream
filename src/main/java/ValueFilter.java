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
class ValueTupleFilter extends ValueFilter{

    @Override
    public boolean filter(Tuple2<String,Integer> subEvent) {
        int i = subEvent.getField(1);
        return i > value;
    }

}

