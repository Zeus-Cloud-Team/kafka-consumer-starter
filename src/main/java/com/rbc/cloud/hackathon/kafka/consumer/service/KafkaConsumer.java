package com.rbc.cloud.hackathon.kafka.consumer.service;

import com.rbc.cloud.hackathon.data.Transactions;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class KafkaConsumer {
    private Logger logger= LoggerFactory.getLogger(KafkaConsumer.class);

    @Value("${topic.name}")
    String topicName;

    @KafkaListener(id="ZeusListener", topics="#{'${topic.name}'}", containerFactory = "ZeusListenerFactory")
    private void listen(final List<ConsumerRecord<String, Transactions>> messages, final Acknowledgment ack) {
        logger.info("Received {} messages, iterating..", messages.size());
        for (ConsumerRecord<String, Transactions> record : messages) {
            String key=record.key();
            Transactions value=record.value();
            ack.acknowledge();
            logger.info(" consumed message : key[{}] = payload[{}]",key,value);
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("Done with this batch");

    }
}
