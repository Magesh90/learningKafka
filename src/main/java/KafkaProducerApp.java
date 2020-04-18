import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerApp {

    public static void main(String[] args) {

        Properties kafkaProperties = new Properties();
        kafkaProperties.put("bootstrap.servers", "localhost:9092");
        kafkaProperties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProperties)) {
            for (int i = 0; i < 50; i++) {
                kafkaProducer.send(new ProducerRecord<>(
                        "my-other-topic",
                        Integer.toString(i),
                        "My Message " + i)
                );
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }
}
