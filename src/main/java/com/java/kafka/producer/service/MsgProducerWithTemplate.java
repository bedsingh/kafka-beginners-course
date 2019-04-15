
/**************************************************************************************************************
 * Module Name: kafka-beginners-course
 * Version Control Block
 * Date         Version    Author          Reviewer       Change Description
 * -----------  ---------  --------------  -------------  -------------------
 * Apr 13, 2019 1.0        Singh Bed       xxxxxxxxx      Created
 * -----------  ---------  --------------  -------------  -------------------
 **************************************************************************************************************/

package com.java.kafka.producer.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**************************************************************************************************************
 * Java File: MessageProducerService.java
 * Author  : Bed Singh
 * Description : This class is use to produce message to given topic
 **************************************************************************************************************/

@Service
public class MsgProducerWithTemplate {

	private static final Logger logger = LogManager.getLogger(MsgProducerWithTemplate.class);

	@Value("${kafka.topic.name}")
	private String TOPIC;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	/**
	 * This method sends the message to the topic
	 * @param message
	 */
	public void sendMessageWithCallback(String message) 
	{
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, message);
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() 
		{
			@Override
			public void onSuccess(SendResult<String, String> result) {
				logger.info("Sent message=[ {} ] with offset=[ {} ] and Topic: [ {} ]", message , result.getRecordMetadata().offset(), result.getRecordMetadata().topic());
			}
			
			@Override
			public void onFailure(Throwable exception) {
				logger.info("Unable to send message=[ {} ] due to : {}", message, exception.getMessage());
			}
		});
	}

	
	public void sendMessage(String message) {
		logger.info("Producing message -> {}", message);    
		this.kafkaTemplate.send(TOPIC, message);
		logger.info("Message sent successfully to Kafka Topic -> {} ", TOPIC);
	}

}
