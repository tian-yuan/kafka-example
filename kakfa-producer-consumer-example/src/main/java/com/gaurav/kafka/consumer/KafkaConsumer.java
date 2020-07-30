package com.gaurav.kafka.consumer;

import com.gaurav.kafka.constants.IKafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * Created by tianyuan on 2020/7/30.
 */
public class KafkaConsumer extends Thread{
    private Consumer<Long, String> consumer;

    public KafkaConsumer(String bootstrapServers, String topics) {
        consumer = ConsumerCreator.createConsumer(bootstrapServers, topics);
    }

    @Override
    public void run() {
        int noMessageToFetch = 0;

        while (isAlive()) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }

            consumerRecords.forEach(record -> {
                System.out.println("Record Key " + record.key());
                System.out.println("Record value " + record.value());
                System.out.println("Record partition " + record.partition());
                System.out.println("Record offset " + record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
    }
}
