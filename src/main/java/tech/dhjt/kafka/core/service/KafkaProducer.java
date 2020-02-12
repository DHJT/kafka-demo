package tech.dhjt.kafka.core.service;

import java.util.concurrent.Future;

import org.springframework.kafka.support.SendResult;

public interface KafkaProducer {

    void send(String topic, String msg);

    Future<SendResult<Integer, String>> ascySend(String topic, String msg);

}
