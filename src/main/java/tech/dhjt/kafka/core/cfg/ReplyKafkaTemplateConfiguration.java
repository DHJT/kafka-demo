package tech.dhjt.kafka.core.cfg;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.requestreply.ReplyingKafkaTemplate;
import org.springframework.kafka.transaction.KafkaTransactionManager;

@Configuration
@EnableKafka
public class ReplyKafkaTemplateConfiguration {
    @Value("${spring.kafka.producer.bootstrap-servers}")
    private String producer;

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String consumer;

    @Bean
    public KafkaMessageListenerContainer<String, String> replyContainer(@Autowired ConsumerFactory consumerFactory) {
        ContainerProperties containerProperties = new ContainerProperties("topic.reply");
        return new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
    }

    @Bean
    public ReplyingKafkaTemplate<String, String, String> replyingKafkaTemplate(
            @Autowired ProducerFactory producerFactory, KafkaMessageListenerContainer replyContainer) {
        ReplyingKafkaTemplate template = new ReplyingKafkaTemplate<>(producerFactory, replyContainer);
        template.setReplyTimeout(10000);
        return template;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Integer, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setReplyTemplate(kafkaTemplate());
        return factory;
    }

    @Bean
    @Primary
    public KafkaTemplate<Integer, String> kafkaTemplate() {
        KafkaTemplate template = new KafkaTemplate<>(producerFactory());
        return template;
    }

    @Bean("txKafkaTemplate")
    public KafkaTemplate<String, String> txKafkaTemplate() {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(txProducerFactory());
        return template;
    }

    @Bean
    public ProducerFactory<Integer, String> producerFactory() {
        ProducerFactory producerFactory = new DefaultKafkaProducerFactory<>(senderProps());
//        producerFactory.transactionCapable();
        return producerFactory;
    }

    @Bean
    public ConsumerFactory<Integer, String> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerProps());
    }

    // 消费者配置参数
    private Map<String, Object> consumerProps() {
        Map<String, Object> props = new HashMap<>();
        // 连接地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumer);
        // GroupID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "replyTest");
        // 是否自动提交
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交的频率
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        // Session超时设置
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000");
        // 键的反序列化方式
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        // 值的反序列化方式
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    // 生产者配置
    private Map<String, Object> senderProps() {
        Map<String, Object> props = new HashMap<>();
        // 连接地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, producer);
        // 重试，0为不启用重试机制
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        // 控制批处理大小，单位为字节
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        // 批量发送，延迟为1毫秒，启用该功能能有效减少生产者发送消息次数，从而提高并发量
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        // 生产者可以使用的总内存字节来缓冲等待发送到服务器的记录
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024000);
        // 键的序列化方式
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        // 值的序列化方式
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return props;
    }

    // 添加事务管理
    @Bean("txProducerFactory")
    public ProducerFactory<String, String> txProducerFactory() {
        DefaultKafkaProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(senderProps());
        factory.transactionCapable();
        factory.setTransactionIdPrefix("tran-");
        return factory;
    }

    @Bean
    public KafkaTransactionManager<?, ?> transactionManager(@Autowired ProducerFactory<?, ?> txProducerFactory) {
        KafkaTransactionManager<?, ?> manager = new KafkaTransactionManager<>(txProducerFactory);
        return manager;
    }

}
