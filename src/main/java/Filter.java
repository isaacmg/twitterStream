import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.List;
/**
 * Created by Isaac Godfried on 1/23/2017.
 */
public class Filter implements TwitterSource.EndpointInitializer, Serializable {
    private static List<String> terms;
    private static List<Location> locations;
    private static List<Long> userIDS;

    Filter(List<String> terms1 ){
        terms = terms1;

    }
    public StreamingEndpoint createEndpoint(){

        StatusesFilterEndpoint status1 = new StatusesFilterEndpoint();
        status1.trackTerms(terms);
        return status1;
    }

}
