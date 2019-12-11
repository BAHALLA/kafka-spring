package org.sid.controllers;


import org.sid.models.PageEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class KafkaController {

    private KafkaTemplate<String, PageEvent> kafkaTemplate;

    public KafkaController(KafkaTemplate<String, PageEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/send/{page}/{topic}")
    public String send(@PathVariable String page,@PathVariable String topic) {
        PageEvent pageEvent = new PageEvent(page, new Date(), new Random().nextInt(100));
        kafkaTemplate.send(topic, pageEvent.getPage(), pageEvent);
        return  "Event coming from page : " + page+ " is published successfully";
    }
}
