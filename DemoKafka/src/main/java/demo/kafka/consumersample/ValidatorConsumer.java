package demo.kafka.consumersample;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ValidatorConsumer {
    public static void main(String[] args) {
        final var consumerProps = new Properties();
        consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "validation-consumer");
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        consumerProps.setProperty(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, Invoice.class.getName());
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "validation-group-id");

        final var consumer = new KafkaConsumer<String, Invoice>(consumerProps);
        consumer.subscribe(List.of("invoice-topic"));

        final var producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "validation-producer");
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        final var producer = new KafkaProducer<>(producerProps);
        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(r -> {
                if (!r.value().isValid()) {
                    producer.send(new ProducerRecord<>("invalid-invoice-topic", r.value().getStoreId(), r.value()));
                    log.info("Invalid record - {}", r.value().getInvoiceNumber());
                    return;
                }
                producer.send(new ProducerRecord<>("valid-invoice-topic", r.value().getStoreId(), r.value()));
                log.info("Valid recode - {}", r.value().getInvoiceNumber());
            });

        }
    }
}
