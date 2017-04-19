// Package name
package kafkaproject.udemy;

// Java Util
import java.util.Properties;

// Imports from Kafka
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {

    public static void main(String[] args) {
        // Name of Topic
        String topicName = "topic1";

        // Producer configuration
        Properties props = new Properties();

        // Add the properties to the instance (k,v)
        props.put("bootstrap.servers", "localhost:9092");

        // Wants acks for all messages
        props.put("acks", "all");

        // Requests fails, the producer tries automatically x times
        props.put("retries", "0");

        // Size of buffer
        props.put("buffer.size", 16324);

        // This reduce the number of request
        props.put("linger.ms", 1);

        //
        props.put("buffer.memory", 33554432);

        //
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 10; i++) {

            producer.send(new ProducerRecord<String, String>(topicName,
                    Integer.toString(i),
                    Integer.toString(i)));
            System.out.println("message sent");

        }
        producer.close();

    }
}
