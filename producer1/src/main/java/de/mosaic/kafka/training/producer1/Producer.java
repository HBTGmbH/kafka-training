package de.mosaic.kafka.training.producer1;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController("/")
public class Producer {

    private KafkaTemplate<String, String> kafkaTemplate;

    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/create-event")
    public void createEvent() {
        kafkaTemplate.send("Topic1", String.valueOf(System.currentTimeMillis()), "value-1");
        System.out.println("Event 'Topic1' has been produced");
    }

    @RequestMapping(method = RequestMethod.GET, path = "/create-event-new")
    public void createEventTopicNew() {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send("Topic-New", String.valueOf(System.currentTimeMillis()), "value-new");
        try {
            System.out.println(" Event was send to topic:" + send.get().getRecordMetadata().topic());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        System.out.println("Event 'Topic-New' has been produced");
    }
}
