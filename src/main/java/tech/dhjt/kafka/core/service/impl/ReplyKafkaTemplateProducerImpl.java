package tech.dhjt.kafka.core.service.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.requestreply.RequestReplyFuture;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import tech.dhjt.kafka.core.service.ReplyKafkaTemplateProducer;

/**
 * @description ReplyingKafkaTemplate 发送消息
 * @author DHJT 2020-02-12 15:32:16
 *
 */
@Slf4j
@Service
public class ReplyKafkaTemplateProducerImpl implements ReplyKafkaTemplateProducer {

    @Autowired
    private ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate;

    @Override
    public void send() throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>("topic.request", "request message");
        record.headers().add(new RecordHeader(KafkaHeaders.REPLY_TOPIC, "topic.reply".getBytes()));
        RequestReplyFuture<String, String, String> replyFuture = replyingKafkaTemplate.sendAndReceive(record);
        SendResult<String, String> sendResult = replyFuture.getSendFuture().get();
        log.info("send request msg result: " + sendResult.getRecordMetadata());
        ConsumerRecord<String, String> consumerRecord = replyFuture.get();
        log.info("receive reply result: " + consumerRecord.value());
    }
}
