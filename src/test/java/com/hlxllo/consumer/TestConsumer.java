package com.hlxllo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;

/**
 * @author hlxllo
 * @description 测试消费者
 * @date 2025/3/26
 */
public class TestConsumer {
    @Test
    public void testSimpleConsumer() {
        HashMap<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        config.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        // 配置消费者读取数据的位置 ，取值为earliest（最早），latest（最晚）
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // 配置消费者组
        config.put("group.id", "hlxllo");
        // 配置自动提交偏移量
        config.put("enable.auto.commit", "true");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)){
            // 消费者订阅主题
            consumer.subscribe(Collections.singletonList("test"));
            while (true) {
                // 每隔100毫秒 拉取一次数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("K = " + record.key() + ", V = " + record.value());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
