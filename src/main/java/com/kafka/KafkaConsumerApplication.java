package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
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

		String kafkaServer = "localhost:9092";
		String kafkaUserName = "";
		String kafkaPassword = "";
		String groupId = "test";
		String topics = "test";

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
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
            props.put(ConsumerConfig.CLIENT_ID_CONFIG, hostname);
            LOGGER.debug("Getting HOSTNAME: " + hostname);
        } catch (UnknownHostException e) {
            // Handle the exception, e.g., fallback to a default client ID
            e.printStackTrace();
        }

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			consumer.subscribe(Collections.singletonList(topics));

			while (true) {
			    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			    for (ConsumerRecord<String, String> record : records) {
			        LOGGER.debug("offset = " + record.offset() + " , key = " +record.key() + " , value = " + record.value());
			    }
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

}
