package tech.dhjt.kafka.core.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import tech.dhjt.kafka.core.service.KafkaProducer;
import tech.dhjt.kafka.core.service.ReplyKafkaTemplateProducer;

@RestController
@RequestMapping("/message")
@Api(tags = "kafka测试接口")
public class TestController {

    @Autowired
    private KafkaProducer producer;

    @Autowired
    private ReplyKafkaTemplateProducer rProducer;

    @GetMapping("/send")
    @ApiImplicitParams({
        @ApiImplicitParam(value = "topic", name = "topic"),
        @ApiImplicitParam(value = "消息内容", name = "msg")
    })
    public String send(String topic, String msg) {
//        producer.send("topic.quick.demo", "123");
        producer.send(topic, msg);
        return "SUCCESS";
    }

    @GetMapping("/send/reply")
    public String send() {
        try {
            rProducer.send();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "SUCCESS";
    }

}
