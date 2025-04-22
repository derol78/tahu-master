package org.eclipse.tahu;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.tahu.message.SparkplugBPayloadDecoder;
import org.eclipse.tahu.message.model.SparkplugBPayload;
import org.eclipse.tahu.message.model.Topic;
import org.eclipse.tahu.util.TopicUtil;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class SparkplugListener implements MqttCallbackExtended {

    // Configuration
    private String serverUrl = "ssl://192.168.20.10:8883"; // Use ssl:// for TLS
    private String clientId = "SparkPlugBDecoder";
    private String username = "test";
    private String password = "test";
    private MqttClient client;

    public static void main(String[] args) {
        SparkplugListener listener = new SparkplugListener();
        listener.run();
    }

    public void run() {
        try {
            // Connect to the MQTT Server
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(30);
            options.setKeepAliveInterval(30);
            options.setUserName(username);
            options.setPassword(password.toCharArray());
            options.setSocketFactory(createSocketFactory()); // Set up TLS with CA certificate

            client = new MqttClient(serverUrl, clientId);
            client.setTimeToWait(5000); // Short timeout on failure to connect
            client.connect(options);
            client.setCallback(this);

            // Just listen to all DDATA messages on spAv1.0 topics and wait for inbound messages
            client.subscribe("spBv1.0/BT00/NDATA/GW2.0", 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SSLSocketFactory createSocketFactory() throws Exception {
        // Load the CA certificate
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (InputStream certInput = new FileInputStream("C:\\Users\\derol\\cert\\ca.crt")) {
            X509Certificate certificate = (X509Certificate) certificateFactory.generateCertificate(certInput);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null); // Initialize the key store
            keyStore.setCertificateEntry("alias", certificate); // Add the CA certificate

            trustManagerFactory.init(keyStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            return sslContext.getSocketFactory();
        }
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        System.out.println("Connected!");
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("The MQTT connection was lost! - will auto-reconnect");
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        Topic sparkplugTopic = TopicUtil.parseTopic(topic);
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(Include.NON_NULL);

        System.out.println("Message arrived on Sparkplug topic " + sparkplugTopic.toString());

        SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
        SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);

        // Convert the message to JSON
        try {
            String payloadString = mapper.writeValueAsString(inboundPayload);

            // Check if the client is connected before publishing
            if (client.isConnected()) {
                MqttMessage publishMessage = new MqttMessage(payloadString.getBytes());
                publishMessage.setQos(1); // Set QoS level (1: At least once)
                client.publish("DECODED/BT00/NDATA/GW2.0", publishMessage);

                System.out.println("Published message to topic '61850_analytic': " + payloadString);
            } else {
                System.out.println("Client is not connected. Cannot publish message.");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        System.out.println("Published message: " + token);
    }
}
