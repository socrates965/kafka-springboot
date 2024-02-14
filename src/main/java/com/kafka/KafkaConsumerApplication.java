package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
public class KafkaConsumerApplication {

	// static final Logger LOGGER = LogManager.getLogger(KafkaConsumerApplication.class);

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaConsumerApplication.class);
	public static void main(String[] args) {
		// SpringApplication.run(KafkaConsumerApplication.class, args);

		final Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.put(SaslConfigs.SASL_JAAS_CONFIG, String.format("%s required username=\"%s\" " + "password=\"%s\";", PlainLoginModule.class.getName(), kafkaUserName, kafkaPassword));
        // props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        // props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		try {
            String hostname = InetAddress.getLocalHost().getHostName();
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, hostname);
            LOGGER.debug("Getting HOSTNAME: " + hostname);
        } catch (UnknownHostException e) {
            // Handle the exception, e.g., fallback to a default client ID
            e.printStackTrace();
        }

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singletonList("test"));

			while (true) {
			    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			    for (ConsumerRecord<String, String> record : records) {
			        LOGGER.debug("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			    }
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
