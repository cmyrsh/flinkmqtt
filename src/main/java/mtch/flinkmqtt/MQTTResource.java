package mtch.flinkmqtt;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by mchaubal on 1/1/18.
 */
public class MQTTResource extends RichParallelSourceFunction<T> {

    public void run(SourceContext<T> sourceContext) throws Exception {

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }


    public void cancel() {

    }
}
