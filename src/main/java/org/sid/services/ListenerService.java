package org.sid.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.sid.models.PageEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ListenerService {
    @KafkaListener(topics = "testTopic", groupId = "group-2")
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {
        PageEvent pageEvent = null;
        try {
            pageEvent = deserializePageEvent(consumerRecord.value());
        }catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        System.out.println("key => "+ consumerRecord.key());
        System.out.println(pageEvent);
    }

    private PageEvent deserializePageEvent(String jsonPageEvent) throws JsonProcessingException {
        JsonMapper jsonMapper = new JsonMapper();
        return jsonMapper.readValue(jsonPageEvent, PageEvent.class);
    }
}
