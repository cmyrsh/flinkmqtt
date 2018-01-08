package mtch.flinkmqtt.conf;

import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

import java.io.Serializable;

/**
 * Created by mchaubal on 1/8/18.
 */
public class MQTTConfig implements Serializable{

    public String getURL() { return null;}

    public String getClientId() { return null;}

    public MqttClientPersistence getPersistance() { return null;}

    public MqttConnectOptions getConnectOptions() { return null;}

    public int getReconnectAttemtCount() { return -1;}

}
