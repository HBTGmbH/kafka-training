package de.hbt.edu.kafka.exampleconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Random;

@Service
public class ExampleConsumer {

    private final KafkaTemplate<String, String> retryExampleKafkaTemplate;

    public ExampleConsumer(KafkaTemplate<String, String> retryExampleKafkaTemplate) {
        this.retryExampleKafkaTemplate = retryExampleKafkaTemplate;
    }

    @KafkaListener(topics = "exampletopic", groupId = "ExampleConsumer.consumeExampletopic")
    public void consumeExampletopic(ConsumerRecord<String, String> record) {
        System.out.println(record.key() + ", " + record.value());
        System.out.println("Partition: " + record.partition() + ", " + record.offset());
        //doSomethingA(record.key(), record.value());
        //doSomethingB(record.key(), record.value());
    }

    @KafkaListener(topics = "exampletopic", groupId = "ExampleConsumer.doSomethingA")
    public void doSomethingA(String key, String value) {
        System.out.println("value length: " + value.length());
    }

    @KafkaListener(topics = { "exampletopic", "ExampleConsumer.doSomethingB.retry" }, groupId = "ExampleConsumer.doSomethingB")
    public void doSomethingB(ConsumerRecord<String, String> record) {
        System.out.println("value hash: " + record.value().hashCode());
        // send to MDL ---
        boolean successful = false; //new Random().nextInt(2) == 0; // mal klappts, mal nicht
        if(successful) {
            // OK - fertig
            System.out.println("success");
        } else {
            // NOK - retry
            long retrycount = 0;
            Header retrycountHeader = record.headers().lastHeader("retrycount");
            if(retrycountHeader != null) {
                retrycount = Long.valueOf(new String(retrycountHeader.value()));
                if(retrycount > 3) {
                    System.err.println("Giving up retries!");
                    return;
                }
            }
            retrycount++;

            System.out.println("retry");
            ProducerRecord<String, String> retryRecord = new ProducerRecord<>("ExampleConsumer.doSomethingB.retry", record.key(), record.value());
            retryRecord.headers().add("retrycount", Long.toString(retrycount).getBytes());
            retryExampleKafkaTemplate.send(retryRecord);
        }
    }

}
