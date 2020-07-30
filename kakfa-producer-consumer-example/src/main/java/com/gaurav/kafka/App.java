package com.gaurav.kafka;

import com.gaurav.kafka.consumer.KafkaConsumer;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

public class App {
	private static DynamicStringProperty kafkaTopics =
			DynamicPropertyFactory.getInstance().getStringProperty("kafka.topics", "");
	private static DynamicStringProperty kafkaBootstrapServers =
			DynamicPropertyFactory.getInstance().getStringProperty("kafka.bootstrap.servers", "");
	private static HashMap<String, KafkaConsumer> consumerMap = new LinkedHashMap<>();

	public static void main(String[] args) throws InterruptedException {
	    System.out.print("start consumer.");
	    System.out.print("topics info :" + kafkaTopics.get());
		kafkaTopics.addCallback(new Runnable() {
			@Override
			public void run() {
				String topics = kafkaTopics.get();
		        System.out.print(topics);
			    List<String> topicList = new ArrayList<>(Arrays.asList(topics.split(",")));
			    topicList.forEach(topic -> {
			        if (!consumerMap.containsKey(topic)) {
			            System.out.print("creat consumer : " + topic);
			            KafkaConsumer consumer = new KafkaConsumer(kafkaBootstrapServers.get(), topic);
			            consumerMap.put(topic, consumer);
			            consumer.start();
			    	}
			    });
			}
		});
		Thread.sleep(30 * 60 * 1000);
	}
}
