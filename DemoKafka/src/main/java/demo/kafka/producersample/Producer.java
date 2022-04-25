package demo.kafka.producersample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {
        final var props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "java-producer");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (var producer = new KafkaProducer<String, String>(props)) {
            for (int i = 0; i < 100; i++) {
                final var message = new ProducerRecord<>("new-kafka-topic", "key-" + i, "message-" + i);
                producer.send(message);
            }
        }
    }
}
