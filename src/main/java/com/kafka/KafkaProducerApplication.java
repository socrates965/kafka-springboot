package com.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.serialization.StringSerializer;
// import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Collections;
import java.util.Properties;
import java.net.InetAddress;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;

@SpringBootApplication
public class KafkaProducerApplication {

	// static final Logger LOGGER = LogManager.getLogger(KafkaProducerApplication.class);

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducerApplication.class);
	public static void main(String[] args) {
		// SpringApplication.run(KafkaProducerApplication.class, args);

		final Properties props = new Properties();

		// Define kafka config
		String kafkaServer = "localhost:9092";
		String kafkaUserName = "";
		String kafkaPassword = "";
		String topic = "test";

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
		props.put(ProducerConfig.RETRIES_CONFIG, "1000");
		props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "1000");
		props.put(ProducerConfig.ACKS_CONFIG, "all");

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// doesnt need to enable if local testing
        // props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), kafkaUserName, kafkaPassword));
        // props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        // props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

		// interceptor configuration, exclusive for confluent platform
        // props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, "io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor");
        // props.put("confluent.monitoring.interceptor.bootstrap.servers",kafkaServer);
        // props.put("confluent.monitoring.interceptor.security.protocol","SASL_SSL");
        // props.put("confluent.monitoring.interceptor.sasl.mechanism","PLAIN");
        // props.put("confluent.monitoring.interceptor.sasl.jaas.config",String.format("%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), "interceptor", "interceptor"));

		// Get Hostname
		try {
            String hostname = InetAddress.getLocalHost().getHostName();
            props.put(ProducerConfig.CLIENT_ID_CONFIG, hostname);
            LOGGER.debug("Getting HOSTNAME: " + hostname);
        } catch (UnknownHostException e) {
            // Handle the exception, e.g., fallback to a default client ID
            e.printStackTrace();
        }

		try {
			// Create a Kafka producer
			Producer<Integer, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(props);
	
			// Create a sample message
			String message = "Hello, Kafka!";
	
			// Send the message to the Kafka topic
			ProducerRecord<Integer, String> record = new ProducerRecord<>(topic, null, message);
			producer.send(record, (metadata, exception) -> {
				if (exception == null) {
					LOGGER.debug("Message sent successfully! Offset: " + metadata.offset());
				} else {
					LOGGER.error("Error sending message: " + exception.getMessage());
				}
			});
	
			// Close the producer
			producer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
