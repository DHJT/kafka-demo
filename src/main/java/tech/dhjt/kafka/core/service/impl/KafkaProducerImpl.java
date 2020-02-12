package tech.dhjt.kafka.core.service.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import lombok.extern.slf4j.Slf4j;
import tech.dhjt.kafka.core.service.KafkaProducer;

/**
 * @description kafka生产者
 * @author DHJT 2020-02-11 14:53:18
 *
 */
@Slf4j
@Service
//@Transactional
public class KafkaProducerImpl implements KafkaProducer {

    @Autowired
    private KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    private KafkaTemplate<String, String> txKafkaTemplate;

    @Override
    public void send(String topic, String msg) {
        log.info("send data:{}, {}", topic, msg);
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, msg);
        txKafkaTemplate.executeInTransaction(new KafkaOperations.OperationsCallback() {
            @Override
            public Object doInOperations(KafkaOperations kafkaOperations) {
                ListenableFuture<SendResult<Integer, String>> future1 = kafkaOperations.send("topic.quick.tran", "test executeInTransaction");
//                throw new RuntimeException("fail");
                future1.addCallback(success -> log.info("KafkaMessageProducer.txKafkaTemplate 发送消息成功！"),
                        fail -> log.error("KafkaMessageProducer.txKafkaTemplate 发送消息失败！"));
                return true;
            }
        });
        future.addCallback(success -> log.info("KafkaMessageProducer 发送消息成功！"),
                fail -> log.error("KafkaMessageProducer 发送消息失败！"));
        SendResult<Integer, String> sendResult;
        try {
            sendResult = future.get();
            System.out.println(sendResult.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Future<SendResult<Integer, String>> ascySend(String topic, String msg) {
        log.info("send data:{}, {}", topic, msg);
        ListenableFuture<SendResult<Integer, String>> future = kafkaTemplate.send(topic, msg);
        future.addCallback(success -> log.info("KafkaMessageProducer 发送消息成功！"),
                fail -> log.error("KafkaMessageProducer 发送消息失败！"));
        SendResult<Integer, String> sendResult;
        try {
            sendResult = future.get();
            System.out.println(sendResult.toString());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return future;
    }

}
