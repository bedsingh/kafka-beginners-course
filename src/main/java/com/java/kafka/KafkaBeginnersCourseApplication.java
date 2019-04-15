
/**************************************************************************************************************
 * Module Name: kafka-beginners-course
 * Version Control Block
 * Date         Version    Author          Reviewer       Change Description
 * -----------  ---------  --------------  -------------  -------------------
 * Apr 13, 2019 1.0        Singh Bed       xxxxxxxxx      Created
 * -----------  ---------  --------------  -------------  -------------------
 **************************************************************************************************************/

package com.java.kafka;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.annotation.EnableKafka;

import com.java.kafka.producer.service.MsgProducerWithCallback;
import com.java.kafka.producer.service.MsgProducerWithTemplate;

/**************************************************************************************************************
 * Java File: KafkaBeginnersCourseApplication.java
 * Author  : Bed Singh
 * Description :  Main application for Kafka Beginners 
 * 
 **************************************************************************************************************/

@EnableKafka 
@SpringBootApplication
public class KafkaBeginnersCourseApplication 
{
	private static final Logger logger = LogManager.getLogger(KafkaBeginnersCourseApplication.class);

	public static void main(String[] args) 
	{
		final String dashLine = "------------------------------------------------------------------------";
		ConfigurableApplicationContext context = null;
		try {
			logger.info(dashLine);
			logger.info("****** Kafka Beginners Course Application Starting, Please Wait...******");
			logger.info(dashLine);

			context = new SpringApplicationBuilder()
					.bannerMode(Banner.Mode.OFF)
					.sources(KafkaBeginnersCourseApplication.class)
					.run(args);

			context.registerShutdownHook();

			logger.info(dashLine);
			logger.info("******* Kafka Beginners Course Application Started Successfully. *******");
			logger.info(dashLine);

			final MsgProducerWithTemplate producer1 = context.getBean(MsgProducerWithTemplate.class);
			final MsgProducerWithCallback producer2 = context.getBean(MsgProducerWithCallback.class);

			IntStream.range(1, 4).forEach(index -> {
				final String message = "This is message number "+index+" with Date - "+LocalDateTime.now();
				producer2.sendMessageWithCallback(message);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) { 	e.printStackTrace(); }
			});

			//Consumer Listener start automatically.
		}
		catch(Exception exception) {
			logger.error("Exception occured: {} ", exception.getMessage(), exception);
		}
		finally {
			Optional.ofNullable(context).ifPresent(ConfigurableApplicationContext :: close);
			logger.info(dashLine);
			logger.info("****** Kafka Beginners Course Application Completed Successfully. ******");
			logger.info(dashLine);
		}

	}


	/*
	 * @Bean public MessageConsumerService getBean() { return new
	 * MessageConsumerService(); }
	 */

}
