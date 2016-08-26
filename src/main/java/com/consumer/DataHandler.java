package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * @author 谢俊权
 * @create 2016/8/25 18:28
 */
public interface DataHandler {

    <K, V> void  handle(String topic, ConsumerRecord<K, V> record);
}
