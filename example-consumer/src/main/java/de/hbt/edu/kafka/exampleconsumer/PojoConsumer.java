package de.hbt.edu.kafka.exampleconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class PojoConsumer {
    public static void main(String[] args) {

        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"http://localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer");
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(configs);

        Collection<String> topics = new HashSet<>();
        topics.add("Topic1");
        consumer.subscribe(topics);

        ConsumerRecords<String, String> events = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        events.forEach(event-> System.err.println("XXXXXXX---XXXXXX"+event.key()+event.value()));
        ConsumerRecords<String, String> events2 = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        events2.forEach(event-> {
            consumer.seek(new TopicPartition(event.topic(),event.partition()),event.offset()-1);
            System.err.println("FEHLER----WWWWWWWWWWWWW");
        });

        consumer.close();

    }
}
