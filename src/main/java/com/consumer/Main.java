package com.consumer;

import com.tools.TopicTool;

/**
 * @author 谢俊权
 * @create 2016/8/19 14:52
 */
public class Main {

    public static void main(String[] args){
//        ConsumerConfig config = new ConsumerConfig.Builder()
//                .bootstrapServers(Arrays.asList("172.19.40.155:9092","172.19.40.155:9093","172.19.40.155:9094"))
//                .groupId("test")
//                .enableAutoCommit(true)
//                .autoCommitIntervalMs(1000L)
//                .sessionTimeoutMs(30000)
//                .maxPollRecords(10)
//                .keyDeserializer(StringDeserializer.class)
//                .valueDeserializer(StringDeserializer.class)
//                .build();
//        MessageConsumer<String, String> consumer = new MessageConsumer<String, String>(
//                config,
//                Arrays.asList("my-topic"),
//                new DataHandler() {
//                    public <K, V> void handle(String topic, ConsumerRecord<K, V> record) {
//                        System.out.printf("topic:%s key:%s value:%s \n", topic, record.key(), record.value());
//                    }
//                });
//        consumer.consume(5000);
//
//
//        ConsumerConfig config1 = new ConsumerConfig.Builder()
//                .bootstrapServers(Arrays.asList("172.19.40.155:9092","172.19.40.155:9093","172.19.40.155:9094"))
//                .groupId("test-1")
//                .enableAutoCommit(true)
//                .autoCommitIntervalMs(1000L)
//                .sessionTimeoutMs(30000)
//                .maxPollRecords(10)
//                .keyDeserializer(StringDeserializer.class)
//                .valueDeserializer(StringDeserializer.class)
//                .build();
//        MessageConsumer<String, String> consumer1 = new MessageConsumer<String, String>(
//                config1,
//                Arrays.asList("my-topic"),
//                new DataHandler() {
//                    public <K, V> void handle(String topic, ConsumerRecord<K, V> record) {
//                        System.out.printf("key:%s value:%s \n", topic, record.key(), record.value());
//                    }
//                });
//        consumer1.consume(5000);

        TopicTool.getInstance().init("172.19.40.155:2181", 2000, 2000);
        TopicTool.getInstance().update("my-topic", 2, 1);
//        TopicTool.getInstance().delete("my-topic");
//        TopicTool.getInstance().create("my-topic", 1, 1);
    }
}
