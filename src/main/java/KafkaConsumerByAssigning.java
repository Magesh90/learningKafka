import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.lang.Thread.currentThread;

public class KafkaConsumerByAssigning {

    public static void main(String[] args) {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.put("bootstrap.servers", "localhost:9092");
        kafkaConsumerProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaConsumerProperties);

        TopicPartition myTopicPartition = new TopicPartition("my-topic", 0);
        TopicPartition myOtherTopicPartition = new TopicPartition("my-other-topic", 2);
        List<TopicPartition> topicPartitionList = new ArrayList<>();
        topicPartitionList.add(myTopicPartition);
        topicPartitionList.add(myOtherTopicPartition);

        kafkaConsumer.assign(topicPartitionList);

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
