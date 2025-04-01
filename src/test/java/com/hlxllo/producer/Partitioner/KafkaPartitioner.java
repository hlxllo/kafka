package com.hlxllo.producer.Partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @author hlxllo
 * @description 自定义分区器
 * @date 2025/3/27
 */
public class KafkaPartitioner implements Partitioner {
    // 根据业务需求计算分区
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        System.out.println("自定义分区器");
        return 0;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
