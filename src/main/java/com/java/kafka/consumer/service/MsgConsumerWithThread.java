
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
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**************************************************************************************************************
 * Java File: MsgMsgConsumerWithThread.java
 * Author  : Bed Singh
 * Description :  Simple consumer class without spring boot.
 * 
 **************************************************************************************************************/

public class MsgConsumerWithThread {

	private static final Logger logger = LogManager.getLogger(MsgConsumerWithThread.class);

	public static void main(String[] args) 
	{
		new MsgConsumerWithThread().run();
	}

	private void run() {
		String bootstrapServers = "127.0.0.1:9092";
		String groupId ="kafka-app-group";
		String topic = "beginners-topic-1";
		CountDownLatch latch = new CountDownLatch(1);

		logger.info("Creating the consumer thread.");
		Runnable consumerRunnable = new ConsumerThread(bootstrapServers, topic, groupId, latch);

		//Start the thread
		Thread thread = new Thread(consumerRunnable);
		thread.start();

		//Add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Cought shutdown hook!");
			((ConsumerThread)consumerRunnable).shutdown();
			try {
				latch.await();
			} 
			catch (InterruptedException exception) {
				logger.error("Application got interrupted. "+exception);
			}

			logger.info("Application has exited!!");
		}));

		try {
			latch.await();
		} 
		catch (InterruptedException exception) {
			logger.error("Application got interrupted. "+exception);
		}
		finally {
			logger.info("Application closing...");
		}
	}

	public class ConsumerThread implements Runnable {

		private CountDownLatch latch;
		private KafkaConsumer<String, String> kafkaConsumer;

		public ConsumerThread(String bootstrapServer, String topic, String groupId, CountDownLatch latch) {
			this.latch = latch;

			//Create consumer config
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest two values 

			//Create consumer
			this.kafkaConsumer = new KafkaConsumer<>(properties);

			//subscriber consumer to our topic(s)
			this.kafkaConsumer.subscribe(Collections.singleton(topic));
			//kafkaConsumer.subscribe(Arrays.asList(topic));

		}

		@Override
		public void run() {

			try {
				//poll for new data
				while(true) {
					ConsumerRecords<String, String> records = this.kafkaConsumer.poll(Duration.ofMillis(3000));

					for(ConsumerRecord<String, String> record : records) {
						logger.info("Key: "+record.key()+" Partition: "+record.partition()+" Offset: "+record.offset()+" Value: "+record.value());
					}
				}
			}
			catch(WakeupException exception) {
				logger.warn("Received shutdown signal!!");
			}
			finally {
				this.kafkaConsumer.close();
				this.latch.countDown();
			}
		}

		public void shutdown() {
			//this wakeup method is a special method to interrupt consumer.poll() and it will through 
			//exception WakeupException
			this.kafkaConsumer.wakeup();
		}
	}
}
