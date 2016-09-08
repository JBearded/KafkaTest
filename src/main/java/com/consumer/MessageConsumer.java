package com.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * @author 谢俊权
 * @create 2016/8/24 19:16
 */
public class MessageConsumer<K, V> {

    private ConsumerConfig config;

    private Consumer<K, V> consumer;

    private DataHandler handler;

    private Timer timer = new Timer();

    private ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue = new ConcurrentLinkedQueue<RecordInfo<K, V>>();

    public MessageConsumer(List<String> topics, DataHandler handler) {
        this(new ConsumerConfig.Builder().build(), topics, handler);
    }

    public MessageConsumer(ConsumerConfig config, List<String> topics, DataHandler handler) {
        this.handler = handler;
        this.config = config;
        this.consumer = new KafkaConsumer<K, V>(config.get());
        this.consumer.subscribe(topics);
        timer.schedule(new HandleMissRecordTask(missRecordQueue), 1000, 2000);
    }

    public void consume(long timeout){
        new Thread(new ConsumeTask(timeout)).start();
    }

    private class ConsumeTask implements Runnable{

        private long timeout;

        public ConsumeTask(long timeout) {
            this.timeout = timeout;
        }

        public void run() {

            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(timeout);
                for(TopicPartition topicPartition : records.partitions()){
                    List<ConsumerRecord<K, V>> list = records.records(topicPartition);
                    for(ConsumerRecord<K, V> record : list){
                        try{
                            handler.handle(topicPartition.topic(), record);
                        }catch (Exception e){
                            RecordInfo<K, V> recordInfo = new RecordInfo<K, V>(topicPartition.topic(), record);
                            missRecordQueue.offer(recordInfo);
                        }
                    }
                }
            }
        }
    }

    private class HandleMissRecordTask extends TimerTask {

        private ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue;

        public HandleMissRecordTask(ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue) {
            this.missRecordQueue = missRecordQueue;
        }

        @Override
        public void run() {
            RecordInfo<K, V> recordInfo = this.missRecordQueue.poll();
            if(recordInfo != null){
                try{
                    handler.handle(recordInfo.topic, recordInfo.record);
                }catch (Exception e){
                    missRecordQueue.offer(recordInfo);
                }
            }

        }
    }

    private class RecordInfo<K, V>{
        private String topic;
        private ConsumerRecord<K, V> record;

        public RecordInfo(String topic, ConsumerRecord<K, V> record) {
            this.topic = topic;
            this.record = record;
        }
    }

}
