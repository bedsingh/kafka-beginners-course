
/**************************************************************************************************************
 * Module Name: kafka-beginners-course
 * Version Control Block
 *
 * Date         Version    Author          Reviewer       Change Description
 * -----------  ---------  --------------  -------------  -------------------
 * Apr 14, 2019 1.0        Singh Bed       xxxxxxxxx      Created
 * -----------  ---------  --------------  -------------  -------------------
 **************************************************************************************************************/

package com.java.kafka.producer.service;

import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**************************************************************************************************************
 * Java File: SimpleKafkaProducer.java
 * Author  : Bed Singh
 * Description : This is another way of creating Producer and send message. 
 * Without kafkaTmplate 
 **************************************************************************************************************/

@Component
public class MsgProducerWithCallback {

	private static final Logger logger = LogManager.getLogger(MsgProducerWithCallback.class);

	@Value("${kafka.topic.name}")
	private String TOPIC;
	
	private KafkaProducer<String, String> producer;

	
	@PostConstruct
	public void init() {
		//Following steps two create kafka producer and send data
		//1. Create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//2. Create Producer 
		this.producer = new KafkaProducer<>(properties);
	}

	public void sendMessageWithCallback(String message) {
		//3. Create Record
		ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);

		//3. Send Data
		producer.send(record, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {

				//Execute every time a record successfully sent or an exception is thrown	
				if(exception == null) {
					String messgae = new StringBuilder()
							.append("Received new metadata: ").append("\n")
							.append("Topic: "+metadata.topic()).append("\n")
							.append("Partition: "+metadata.partition()).append("\n")
							.append("Offset: "+metadata.offset()).append("\n")
							.append("Timestamp: "+metadata.timestamp()).toString();
					logger.info(messgae);
				}
				else {
					logger.error("Error while producing message: ", exception);
				}

				logger.info("Sent message=[ {} ] with offset=[ {} ] and Topic: [ {} ]", message , metadata.offset(), metadata.topic());
			}
		});
	}

	
	@PreDestroy
	public void destroy() {
		//4. flush data 
		producer.flush();

		//5. flush and close producer
		producer.close();

	}
}
