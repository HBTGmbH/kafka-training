package de.mosaic.kafka.training.producer1;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController("/")
public class Producer {

    private KafkaTemplate<String,String> kafkaTemplate;

    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @RequestMapping(method = RequestMethod.GET,path = "/create-event")
    public void createEvent(){
        kafkaTemplate.send("Topic1","key-1","value-1");
    }
}
