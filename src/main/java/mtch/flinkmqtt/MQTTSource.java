package mtch.flinkmqtt;

import com.fasterxml.jackson.databind.ObjectMapper;
import mtch.flinkmqtt.conf.MQTTConfig;
import mtch.flinkmqtt.util.FormatManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by mchaubal on 1/1/18.
 *
 * specify correct inputs for MQTT
 * @author Mayuresh Chaubal
 */
public class MQTTSource<OUT, T extends ObjectMapper> extends RichSourceFunction<OUT> implements MqttCallback, Serializable{


    private final static Logger log = LoggerFactory.getLogger(MQTTSource.class);

    private volatile boolean isRunning = false;

    private final String topic;

    private final int qos;

    private final ArrayBlockingQueue<OUT> buffer;

    private final FormatManager<OUT, T> fmtmanager;

    private MqttAsyncClient client;

    private final MQTTConfig conf;



    public MQTTSource(MQTTConfig conf, int capacity, int qos, String topic) throws IllegalAccessException, InstantiationException {

        this.conf = conf;

        this.qos = qos;

        this.topic = topic;

        this.fmtmanager = new FormatManager<OUT, T>();

        buffer = new ArrayBlockingQueue<OUT>(capacity);
    }

    public void run(SourceContext<OUT> sourceContext) throws Exception {
        log.debug("Running source");

        while (isRunning) {
            sourceContext.collect(buffer.take());
        }
     }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if(connect()) {
            isRunning = true;
        }
    }


    private final boolean connect() throws Exception{


        client = new MqttAsyncClient(
                conf.getURL(),
                conf.getClientId(),
                conf.getPersistance());


        client.setCallback(this);

        // set user name and password if provided
       log.info(String.format("MQTT Client connected. URL [%s] ClientId [%s]", client.getServerURI(), client.getClientId()));

        IMqttToken token = client.connect(conf.getConnectOptions());

        token.waitForCompletion();

        client.subscribe(topic, qos);

        return client.isConnected();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(null != client && client.isConnected()) {
           client.disconnect();

           log.info("MQTT Client disconnected");
        }
        client.close();
        log.info("MQTT Client closed");



    }


    public void cancel() {
        log.info("Cancel called");
    }

    public void connectionLost(Throwable throwable) {
        log.error("MQTT Connection Lost.", throwable);
        if(!reconnectAttempt()) {
            throw new RuntimeException("Unable to connect back to MQTT");
        }
    }

    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        log.debug(String.format("New message from %s. Message[%s]", s, new String(mqttMessage.getPayload())));
        if(s.equals(topic)) {
            processMessage(s, mqttMessage);
        }

    }


    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

        log.debug("Delivery complete. " + iMqttDeliveryToken.toString());

    }


    private final void processMessage(String topic, MqttMessage message) throws IOException {
        buffer.add(fmtmanager.build(message.getPayload()));
    }
    private final boolean reconnectAttempt() {

        int attemptcount = conf.getReconnectAttemtCount();

        boolean reconnected = false;

        for (int i = 0; i < attemptcount; i++) {

            try {
                if(!(reconnected = connect())) {

                    try{
                        client.disconnect();
                    } catch (Exception e) {
                        log.warn("Ignoring exception while disconnecting", e);
                    }
                }
            } catch (Exception e) {
                log.error("Error while reconnecting.", e);
            }

        }

        return reconnected;
    }


}
