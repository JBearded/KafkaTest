package com.producer;

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

        MessageProducer producer = new MessageProducer(config);
        producer.init();
        for(int i = 0; i < 100; i++){
            producer.send("my-topic", String.valueOf(i), "topic-" + String.valueOf(i));
        }
        producer.flush();
        producer.close();
    }
}
