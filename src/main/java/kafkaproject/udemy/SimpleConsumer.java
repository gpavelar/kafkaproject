/**
 * Created by gavelar on 11/04/17.
 * BIGSEA-PROJECT
 */

// Package name
package kafkaproject.udemy;

//import org.apache.kafka.clients.consumer.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    public static void main(String[] args) {

        String topicName = "topic1";

        // Adding the properties of Kafka consumer
        Properties props = new Properties();

        //
        props.put("bootstrap.servers", "localhost:9092");

        // GroupIP
        props.put("group.id", "test");

        //
        props.put("enable.auto.commit", "true");

        //
        props.put("auto.commit.interval.ms", "1000");

        // Session timeout
        props.put("session.timeout.ms", "30000");

        //
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        //
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<String, String>(props);

        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("message is  received from " + topicName);

        int i = 0;

        // Always the consumer it will wait for the message every 1000 seconds
        // Store records
        while (true) {
            // Consumer Records
            ConsumerRecords<String, String> records = consumer.poll(100);

            // Loop in records
            // Read each record from records
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Message:" + record);

            }
        }
    }
}
