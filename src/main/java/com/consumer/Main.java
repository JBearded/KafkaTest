package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;

/**
 * @author 谢俊权
 * @create 2016/8/19 14:52
 */
public class Main {

    public static void main(String[] args){
        ConsumerConfig config = new ConsumerConfig.Builder()
                .bootstrapServers(Arrays.asList("172.19.40.155:9092","172.19.40.155:9093","172.19.40.155:9094"))
                .groupId("test")
                .enableAutoCommit(true)
                .autoCommitIntervalMs(1000L)
                .sessionTimeoutMs(30000)
                .maxPollRecords(10)
                .keyDeserializer(StringDeserializer.class)
                .valueDeserializer(StringDeserializer.class)
                .build();
        MessageConsumer<String, String> consumer = new MessageConsumer<String, String>(
                config,
                Arrays.asList("my-topic"),
                new DataHandler() {
                    public <K, V> void handle(String topic, ConsumerRecord<K, V> record) {
                        System.out.printf("topic:%s key:%s value:%s \n", topic, record.key(), record.value());
                    }
                });
        consumer.init();
        consumer.consume(5000);

    }
}
