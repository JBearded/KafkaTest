package com.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 谢俊权
 * @create 2016/8/19 14:52
 */
public class Main {

    public static void main(String[] args){
        manualOffsetControl();
    }

    public static void automaticOffsetCommitting(){

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
        Consumer consumer = new KafkaConsumer(config.get());
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(60000);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("consumer: offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            if(!records.isEmpty()){
                System.out.println();
            }
        }
    }

    public static void manualOffsetCommitting(){
        ConsumerConfig config = new ConsumerConfig.Builder()
                .bootstrapServers(Arrays.asList("172.19.40.155:9092","172.19.40.155:9093","172.19.40.155:9094"))
                .groupId("test")
                .enableAutoCommit(false)
                .autoCommitIntervalMs(1000L)
                .sessionTimeoutMs(30000)
                .maxPollRecords(10)
                .keyDeserializer(StringDeserializer.class)
                .valueDeserializer(StringDeserializer.class)
                .build();
        Consumer consumer = new KafkaConsumer(config.get());
        consumer.subscribe(Arrays.asList("my-topic"));
        int minBatchSize = 20;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for(ConsumerRecord<String, String> record : records){
                buffer.add(record);
                if(buffer.size() >= minBatchSize){
                    insertIntoDB(buffer);
                    consumer.commitSync();
                    buffer.clear();
                }
            }
        }
    }

    public static void insertIntoDB(List<ConsumerRecord<String, String>> buffer) {
        for (ConsumerRecord<String, String> record : buffer)
            System.out.printf("consumer: offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value());
        if(!buffer.isEmpty()){
            System.out.println();
        }
    }

    public static void manualOffsetControl(){
        ConsumerConfig config = new ConsumerConfig.Builder()
                .bootstrapServers(Arrays.asList("172.19.40.155:9092","172.19.40.155:9093","172.19.40.155:9094"))
                .groupId("test")
                .enableAutoCommit(true)
                .autoCommitIntervalMs(1000L)
                .sessionTimeoutMs(30000)
                .maxPollRecords(100)
                .keyDeserializer(StringDeserializer.class)
                .valueDeserializer(StringDeserializer.class)
                .build();
        Consumer consumer = new KafkaConsumer(config.get());
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            for(TopicPartition topicPartition : records.partitions()){
                List<ConsumerRecord<String, String>> list = records.records(topicPartition);
                long offset = handle(list);
                if(offset > 0){
                    consumer.seek(topicPartition, offset);
                }
            }
        }
    }

    public static AtomicInteger times = new AtomicInteger(0);
    public static long handle(List<ConsumerRecord<String, String>> list) {
        long offset = -1;
        for (int i = 0; i < list.size(); i++) {
            ConsumerRecord<String, String> record = list.get(i);
            if(i == list.size()/2 && times.getAndIncrement() == 0){
                offset = record.offset();
            }
            System.out.printf("offset = %d, key = %s, value = %s ", record.offset(), record.key(), record.value());
        }
        System.out.println();
        return offset;
    }


}
