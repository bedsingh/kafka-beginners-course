
/**************************************************************************************************************
 * Module Name: kafka-beginners-course
 * Version Control Block
 * Date         Version    Author          Reviewer       Change Description
 * -----------  ---------  --------------  -------------  -------------------
 * Apr 13, 2019 1.0        Singh Bed       xxxxxxxxx      Created
 * -----------  ---------  --------------  -------------  -------------------
 **************************************************************************************************************/

package com.java.kafka.consumer.service;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**************************************************************************************************************
 * Java File: MessageConsumerService.java
 * Author  : Bed Singh
 * Description : This class is use to consume message from the given topic for particular group.  
 **************************************************************************************************************/

@Service
public class MsgConsumerWithKafkaListener {
	
	private static final Logger logger = LogManager.getLogger(MsgConsumerWithKafkaListener.class);
		
    @KafkaListener(
    		topics = "${kafka.topic.name}", 
    		groupId = "${spring.kafka.consumer.group-id}", 
    		autoStartup = "true")
    public void consumeMessage(@Payload String message) throws IOException 
    {
        logger.info("Consumed message -> {}", message);
    }
}
