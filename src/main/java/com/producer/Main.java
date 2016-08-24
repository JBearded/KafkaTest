package com.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;

/**
 * @author 谢俊权
 * @create 2016/8/19 10:23
 */
public class Main {


    public static void main(String[] args){

        ProducerConfig config = new ProducerConfig
                .Builder()
                .bootstrapServers(Arrays.asList("172.19.40.155:9092", "172.19.40.155:9093", "172.19.40.155:9094"))
                .acks("all")
                .retries(3)
                .lingerMs(1L)
                .bufferMemory(33554432L)
                .keySerializer(StringSerializer.class)
                .valueSerializer(StringSerializer.class)
                .build();

        Producer producer = new KafkaProducer(config.get());
        for(int i = 0; i < 100; i++){
            producer.send(new ProducerRecord("my-topic", String.valueOf(i), "topic-" + String.valueOf(i)));
        }
        producer.close();
    }
}
