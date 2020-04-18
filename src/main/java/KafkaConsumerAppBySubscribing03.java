import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.currentThread;

public class KafkaConsumerAppBySubscribing03 {
    public static void main(String[] args) {

        Properties kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", "localhost:9092");
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("group.id", "test");

        List<String> topics = new ArrayList<>();
        //topics.add("my-topic");
        topics.add("my-other-topic");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaConsumerProps);
        kafkaConsumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(10);

                for (ConsumerRecord consumerRecord : consumerRecords) {
                    System.out.println(
                            String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s, Thread: %s",
                                    consumerRecord.topic(),
                                    consumerRecord.partition(),
                                    consumerRecord.offset(),
                                    consumerRecord.key(),
                                    consumerRecord.value(),
                                    currentThread().getName())
                    );

                }
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
