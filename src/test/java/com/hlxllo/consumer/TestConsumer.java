package com.hlxllo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Set;

/**
 * @author hlxllo
 * @description 测试消费者
 * @date 2025/3/26
 */
public class TestConsumer {

    static HashMap<String, Object> config = new HashMap<>();

    static {
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
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "hlxllo");
        // 配置自动提交偏移量(如果开启手动提交需要禁用)
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    }

    // 简单消费者
    @Test
    public void testSimpleConsumer() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
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

    // 指定偏移量消费
    @Test
    public void testConsumerOffset() {
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList("test"));
            // 配置需要消费的主题和偏移量
            consumer.poll(Duration.ofMillis(100));
            Set<TopicPartition> assignment = consumer.assignment();
            for (TopicPartition topicPartition : assignment) {
                if (topicPartition.topic().equals("test")) {
                    // 获取对应主题的分区后，从头开始读取
                    consumer.seek(topicPartition, 0);
                }
            }
            // 拉取数据
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("K = " + record.key() + ", V = " + record.value());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 手动提交(异步&同步)
    @Test
    public void testConsumerCommit() {
        // 禁用自动提交偏移量
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config)) {
            consumer.subscribe(Collections.singletonList("test"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("K = " + record.key() + ", V = " + record.value());
                }
                // 异步提交
                consumer.commitAsync();
                // 同步提交
                //consumer.commitSync();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
