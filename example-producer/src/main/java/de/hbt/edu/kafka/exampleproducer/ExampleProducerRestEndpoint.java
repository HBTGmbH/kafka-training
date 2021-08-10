package de.hbt.edu.kafka.exampleproducer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;

@RestController("/")
public class ExampleProducerRestEndpoint {

    private final KafkaTemplate<String, String> exampleKafkaTemplate;

    public ExampleProducerRestEndpoint(KafkaTemplate<String, String> exampleKafkaTemplate) {
        this.exampleKafkaTemplate = exampleKafkaTemplate;
    }

    @RequestMapping(method = RequestMethod.POST, path = "/create-event")
    public void createEvent() throws ExecutionException, InterruptedException {
        String key = Long.toString(new Random().nextInt(10));
        String value = "Created " + new Date();

        ListenableFuture<SendResult<String, String>> future = exampleKafkaTemplate.send("exampletopic", key, value);
        SendResult<String, String> result = future.get();

        System.out.println("Event created! key="+key+", value=" + value +", partition="+result.getRecordMetadata().partition()+", offset="+result.getRecordMetadata().offset());
    }

}
