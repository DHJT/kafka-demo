package tech.dhjt.kafka.core.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

/**
 * @description ReplyKafkaTemplate消费并有返回值
 * @author DHJT 2020-02-11 17:30:40
 *
 */
@Component
@Slf4j
public class ReplyKafkaTemplateConsumer {

    @KafkaListener(id = "replyConsumer", topics = "topic.request", containerFactory = "kafkaListenerContainerFactory")
    @SendTo
    public String replyListen(ConsumerRecord<?, ?> record) {
        log.info("topic-{}, offset-{}, value-{}", record.topic(), record.offset(), record.value());
        return "reply message";
    }

    @KafkaListener(id = "replyConsumer1", topics = "topic.request", containerFactory = "kafkaListenerContainerFactory")
    @SendTo("topic.quick.real")
    public String replyListen1(ConsumerRecord<?, ?> record) {
        log.info("topic1-{}, offset1-{}, value1-{}", record.topic(), record.offset(), record.value());
        return "reply message: 这是完成之后发送给replyConsumer2的消息。";
    }

    @KafkaListener(id = "replyConsumer2", topics = "topic.quick.real", containerFactory = "kafkaListenerContainerFactory")
    public String replyListen2(ConsumerRecord<?, ?> record) {
        log.info("topic2-{}, offset2-{}, value2-{}", record.topic(), record.offset(), record.value());
        return "reply message: 这个是replyListen2 方法的返回值";
    }
}
