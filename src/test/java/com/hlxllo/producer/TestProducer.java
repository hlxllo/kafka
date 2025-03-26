package com.hlxllo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.HashMap;

/**
 * @author hlxllo
 * @description 测试生产者
 * @date 2025/3/26
 */
public class TestProducer {
    @Test
    public void testSimpleProducer() {
        // 属性集合，注意value类型为object是定死的
        HashMap<String, Object> config = new HashMap<>();
        // 配置kafka集群地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 配置键值序列化器
        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 创建生产者
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            // 准备数据
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "test", "key1", "value1");
            // 发送数据
            producer.send(record);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
