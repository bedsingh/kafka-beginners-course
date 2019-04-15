
/**************************************************************************************************************
 * Module Name: kafka-beginners-course
 * Version Control Block
 * Date         Version    Author          Reviewer       Change Description
 * -----------  ---------  --------------  -------------  -------------------
 * Apr 14, 2019 1.0        Singh Bed       xxxxxxxxx      Created
 * -----------  ---------  --------------  -------------  -------------------
 **************************************************************************************************************/

package com.java.kafka.consumer.service;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**************************************************************************************************************
 * Java File: SimpleMsgConsumer.java
 * Author  : Bed Singh
 * Description :  
 **************************************************************************************************************/

public class SimpleMsgConsumer {

	private static final Logger logger = LogManager.getLogger(SimpleMsgConsumer.class);


	public static void main(String[] args) {

		String bootstrapServers = "127.0.0.1:9092";
		String groupId ="kafka-app-group";
		String topic = "beginners-topic-1";
		//Create consumer config
		Properties properties = new Properties();

		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest two values 

		//Create consumer
		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

		//subscriber consumer to our topic(s)
		kafkaConsumer.subscribe(Collections.singleton(topic));
		//kafkaConsumer.subscribe(Arrays.asList(topic));

		//poll for new data
		while(true) {
			ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

			for(ConsumerRecord<String, String> record : records) {
				logger.info("Key: "+record.key()+" Partition: "+record.partition()+" Offset: "+record.offset()+" Value: "+record.value());
			}
		}

	}

}
