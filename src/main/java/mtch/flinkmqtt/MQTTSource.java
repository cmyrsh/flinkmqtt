package mtch.flinkmqtt;

import mtch.flinkmqtt.util.FormatManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by mchaubal on 1/1/18.
 */
public class MQTTSource<OUT> extends RichSourceFunction<OUT> implements MqttCallback, Serializable{


    private final Logger log = LoggerFactory.getLogger(MQTTSource.class);

    private volatile boolean isRunning = false;

    private final Set<String> topicset = new LinkedHashSet<String>();
    private final int[] qoslist;

    private final ArrayBlockingQueue<OUT> buffer;
//    private String content      = "Message from MqttPublishSample";
//    private int qos             = 2;
//    private String broker       = "tcp://iot.eclipse.org:1883";
//    private String clientId     = "JavaSample";
//    private MemoryPersistence persistence = new MemoryPersistence();

    private final FormatManager<OUT> fmtmanager;

    private MqttAsyncClient client;

    private final Configuration flinkConfig;


    public MQTTSource(Configuration flinkConfig, int capacity, FormatManager fmtmanager, int qos, String... topics) {
        for (String topic: topics) {
            if(topicset.add(topic)) {
                //qoslist.add(qos);
            }
        }

        this.flinkConfig = flinkConfig;
        this.fmtmanager = fmtmanager;
        buffer = new ArrayBlockingQueue<OUT>(capacity);

        qoslist = new int[topicset.size()];

        for (int i = 0; i < qoslist.length; i++) {
            qoslist[i] = qos;
        }
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
                flinkConfig.getString("mqtt.server.url", "tcp://1883"),
                flinkConfig.getString("mqtt.client.id", getRuntimeContext().getTaskName()),
                flinkConfig.getString("mqtt.persistance", "memory")
                        .equalsIgnoreCase("file") ?
                        new MqttDefaultFilePersistence(flinkConfig.getString("mqtt.persistance.dir", System.getProperty("java.io.tmpdir"))) :
                        new MemoryPersistence());


        client.setCallback(this);

        final MqttConnectOptions option = new MqttConnectOptions();

        option.setCleanSession(flinkConfig.getBoolean("mqtt.session.clean", false));


        // set user name and password if provided

        if(flinkConfig.containsKey("mqtt.user")) {
            option.setUserName(flinkConfig.getString("mqtt.user", ""));
            if(flinkConfig.containsKey("mqtt.password")) {
                option.setPassword(flinkConfig.getString("mqtt.password", "").toCharArray());
            }
        }
        log.info(String.format("MQTT Client connected. URL [%s] ClientId [%s]", client.getServerURI(), client.getClientId()));

        IMqttToken token = client.connect(option);

        token.waitForCompletion();

        client.subscribe(new ArrayList<String>(topicset).toArray(new String[topicset.size()]),
               qoslist);

        return client.isConnected();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(null != client && client.isConnected()) {
           client.disconnect();
           client.close();
           log.info("MQTT Source closed");
        }





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
        if(topicset.contains(s)) {
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

        int attemptcount = flinkConfig.getInteger("mqtt.reconnect.attempt", 2);

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
