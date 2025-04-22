package org.eclipse.tahu;

import org.eclipse.paho.client.mqttv3.*;
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
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

public class SparkplugListener implements MqttCallbackExtended {

    // Configuration loaded from properties
    private String serverUrl;
    private String clientId;
    private String username;
    private String password;
    private String caCertPath;
    private String subTopic;
    private String pubTopic;
    private boolean cleanSession;
    private boolean debug;
    private int inFlight;
    private int subQoS;
    private int pubQoS;
    private MqttClient client;

    public static void main(String[] args) {
        SparkplugListener listener = new SparkplugListener();
        listener.loadConfigAndRun();
    }

    public void loadConfigAndRun() {
        Properties properties = new Properties();
        try {
            // Load configuration from the properties file
            properties.load(new FileInputStream("src/main/resources/config.properties"));

            serverUrl = properties.getProperty("serverUrl", "ssl://localhost:8883");
            clientId = properties.getProperty("clientId", "SparkPlugBDecoderTahuApp");
            username = properties.getProperty("username", "test");
            password = properties.getProperty("password", "test");
            caCertPath = properties.getProperty("caCertPath", "C:\\Users\\derol\\cert\\ca.crt");
            cleanSession = Boolean.parseBoolean(properties.getProperty("cleanSession", "false"));
            debug = Boolean.parseBoolean(properties.getProperty("debug", "false"));
            inFlight = Integer.parseInt(properties.getProperty("inFlight", "100"));
            subTopic = properties.getProperty("subTopic", "spBv1.0/BT00/NDATA/GW2.0");
            subQoS = Integer.parseInt(properties.getProperty("subQoS", "1"));
            pubQoS = Integer.parseInt(properties.getProperty("pubQoS", "1"));
            pubTopic = properties.getProperty("pubTopic", "DECODED/BT00/NDATA/GW2.0");

            run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(cleanSession);
            options.setConnectionTimeout(30);
            options.setKeepAliveInterval(30);
            options.setUserName(username);
            options.setPassword(password.toCharArray());
            options.setSocketFactory(createSocketFactory());
            options.setMaxInflight(inFlight);
            client = new MqttClient(serverUrl, clientId);
            client.setTimeToWait(5000); // Short timeout on failure to connect
            client.connect(options);
            client.setCallback(this);
            client.subscribe(subTopic, subQoS); // QoS = 1

            // Schedule publishing of in-flight message status
            startInFlightStatusPublisher();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startInFlightStatusPublisher() {
        Timer timer = new Timer(true); // Create a daemon thread for periodic tasks
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                publishInFlightStatus();
            }
        }, 0, 5000); // Publish every 5 seconds
    }

    private void publishInFlightStatus() {
        try {
            int currentInFlightMessages = client.getPendingDeliveryTokens().length;
	            if (currentInFlightMessages>0) {
		            String inFlightTopic = "monitoring/" + clientId + "/inFlight";
		            MqttMessage message = new MqttMessage(String.valueOf(currentInFlightMessages).getBytes());
		            message.setQos(1); // Ensure delivery at least once
		            client.publish(inFlightTopic, message);
		
		            if (debug) {
		                System.out.println("Published in-flight message count (" + currentInFlightMessages + ") to topic: " + inFlightTopic);
	            }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private SSLSocketFactory createSocketFactory() throws Exception {
        // Load the CA certificate
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (InputStream certInput = new FileInputStream(caCertPath)) {
            X509Certificate certificate = (X509Certificate) certificateFactory.generateCertificate(certInput);

            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
            keyStore.load(null, null);
            keyStore.setCertificateEntry("alias", certificate);
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
        if (debug) {
            System.out.println("Message arrived on Sparkplug topic " + sparkplugTopic.toString());
        }
        SparkplugBPayloadDecoder decoder = new SparkplugBPayloadDecoder();
        SparkplugBPayload inboundPayload = decoder.buildFromByteArray(message.getPayload(), null);

        // Convert the message to JSON and publish to another topic
        try {
            String payloadString = mapper.writeValueAsString(inboundPayload);
            MqttMessage publishMessage = new MqttMessage(payloadString.getBytes());
            publishMessage.setQos(pubQoS);
            client.publish(pubTopic, publishMessage);
            if (debug) {
                System.out.println("Published message to topic DECODED/BT00/NDATA/GW2.0 : " + payloadString);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        if (debug) {
            System.out.println("Published message: " + token);
        }
    }
}
