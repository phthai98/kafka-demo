package demo.kafka.producersample;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class TransactionProducer {
    public static void main(String[] args) {
        final var props = new Properties();
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "java-transaction-producer");
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id-1");

        final var transactionProducer = new KafkaProducer<String, String>(props);
        transactionProducer.initTransactions();

        try {
            transactionProducer.beginTransaction();
            transactionProducer.send(new ProducerRecord<>("transaction-topic-1", "Message 2 to topic 1"));
            transactionProducer.send(new ProducerRecord<>("transaction-topic-2", "Message 2 to topic 2"));
//            sendMessageError();
            transactionProducer.commitTransaction();
        } catch (Exception ex) {
            transactionProducer.abortTransaction();
            transactionProducer.close();
//            throw new RuntimeException();
            System.out.println(ex.getMessage());
        }
        transactionProducer.close();
    }

    private static void sendMessageError() throws RuntimeException{
        throw new RuntimeException("Co loi");
    }
}
