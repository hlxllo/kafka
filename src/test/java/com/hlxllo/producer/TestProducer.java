package com.hlxllo.producer;

import com.hlxllo.producer.Interceptor.KafkaInterceptor;
import com.hlxllo.producer.Partitioner.KafkaPartitioner;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.junit.Test;

import java.util.HashMap;
import java.util.concurrent.Future;

/**
 * @author hlxllo
 * @description 测试生产者
 * @date 2025/3/26
 */
public class TestProducer {

    // 属性集合，注意value类型为object是定死的
    final static HashMap<String, Object> config = new HashMap<>();

    static {
        // 配置kafka集群地址
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 配置键值序列化器
        config.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        config.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // 配置自定义拦截器
        config.put(
                ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                KafkaInterceptor.class.getName());
        // 配置自定义分区器
        config.put(
                ProducerConfig.PARTITIONER_CLASS_CONFIG,
                KafkaPartitioner.class.getName());
    }

    // 简单发送
    @Test
    public void testSimpleProducer() {
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

    // 异步回调发送，后处理方法异步执行
    @Test
    public void testProducerAsync() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "test", "key" + i, "value" + i);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // 数据发送成功后，会执行此方法
                        System.out.println(recordMetadata.timestamp());
                    }
                });
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 同步回调发送，后处理方法同步执行
    @Test
    public void testProducerSync() {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            for (int i = 0; i < 10; i++) {
                // 多加个get变成同步等待
                ProducerRecord<String, String> record = new ProducerRecord<>(
                        "test", "key" + i, "value" + i);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // 数据发送成功后，会执行此方法
                        System.out.println(recordMetadata.timestamp());
                    }
                }).get();
                // 发送当前数据
                System.out.println("发送数据");
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 事务消息
    @Test
    public void testProducerTransaction() {
        // 配置幂等性
        config.put(
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true);
        // 配置事务id
        config.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                "hlxllo-id");
        // 配置事务超时时间
        config.put(
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5000
        );
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            // 初始化事务
            producer.initTransactions();
            try {
                // 启动事务
                producer.beginTransaction();
                for (int i = 0; i < 10; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            "test", "key" + i, "value" + i);
                    Future<RecordMetadata> result = producer.send(record);
                }
                int a = 1 / 0;
                // 提交事务
                System.out.println("事务提交成功");
                producer.commitTransaction();
            } catch (Exception e) {
                // 终止事务
                System.err.println("事务终止");
                producer.abortTransaction();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
