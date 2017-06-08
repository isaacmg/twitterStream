/**
 * Created by Isaac Godfried on 1/23/2017.
 */
import com.twitter.hbc.core.endpoint.DefaultStreamingEndpoint;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class TwitterSourceOpt extends TwitterSource  {
    private static List<String> terms;
    private EndpointInitializer initializer = new FilterEndpoint(terms);
    TwitterSourceOpt(Properties props, List<String> terms1){
        super(props);
        terms = terms1;



    }
    public static class FilterEndpoint implements EndpointInitializer, Serializable{
        private static List<String> terms;
        FilterEndpoint(List<String> terms1 ){
            terms = terms1;

        }
        public StreamingEndpoint createEndpoint(){

            StatusesFilterEndpoint status1 = new StatusesFilterEndpoint();

            if(terms!=null){status1.trackTerms(terms);}

            return status1;
        }

    }

}
