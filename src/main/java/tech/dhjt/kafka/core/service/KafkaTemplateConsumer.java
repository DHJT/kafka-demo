package tech.dhjt.kafka.core.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @description kafka消费者
 * @author DHJT 2020-02-11 16:15:16
 */
@Component
@Slf4j
public class KafkaTemplateConsumer {

    // 声明consumerID为demo，监听topicName为topic.quick.demo的Topic
    @KafkaListener(id = "demo", topics = "topic.quick.demo")
    public void listen(String msgData) {
        log.info("demo receive : " + msgData);
    }

//    @KafkaListener(id = "replyConsumer", topics = "topic.request", containerFactory = "kafkaListenerContainerFactory")
//    @SendTo
//    public String replyListen(ConsumerRecord<?, ?> record) {
//        log.info("topic-{}, offset-{}, value-{}", record.topic(), record.offset(), record.value());
//        return "reply message";
//    }

    @KafkaListener(id = "kafka", topicPartitions = {@TopicPartition(topic = "test1", partitions = { "0", "1" })})
    public void listen (ConsumerRecord<?, ?> record) {
        log.info("start consume");
        log.info("topic-{}, offset-{}, value-{}", record.topic(), record.offset(), record.value());
    }

}
