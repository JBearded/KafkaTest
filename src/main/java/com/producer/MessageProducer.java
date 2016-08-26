package com.producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;

/**
 * @author 谢俊权
 * @create 2016/8/19 10:23
 */
public class MessageProducer<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    private Timer timer = new Timer();

    private Producer<K, V> producer;

    private ProducerConfig config;

    private ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue = new ConcurrentLinkedQueue<RecordInfo<K, V>>();

    public MessageProducer() {
        this(new ProducerConfig.Builder().build());
    }

    public MessageProducer(ProducerConfig config) {
        this.config = config;
        this.producer = new KafkaProducer<K, V>(this.config.get());
    }

    public void init(){
        this.timer.schedule(new HandleMissRecordTask(this.missRecordQueue), 0, 2000);
    }

    public RecordMetadata send(String topic, K key, V value){
        return send(topic, key, value, null);
    }

    public RecordMetadata send(String topic, K key, V value, Callback callback){
        RecordMetadata recordMetadata = null;
        RecordInfo<K, V> recordInfo = new RecordInfo<K, V>(topic, key, value, callback);
        try{
            ProducerRecord<K, V> record = new ProducerRecord(topic, key, value);
            Future<RecordMetadata> future = this.producer.send(record, callback);
            recordMetadata = future.get(30, TimeUnit.SECONDS);
        }catch (Exception e){
            logger.error("error to send message. topic:{} key:{} value:{} ", topic, key, value, e);
            missRecordQueue.offer(recordInfo);
        }
        return recordMetadata;
    }

    public void flush(){
        this.producer.flush();
    }

    public void close(){
        this.producer.close();
    }


    private class HandleMissRecordTask extends TimerTask{

        private ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue;

        public HandleMissRecordTask(ConcurrentLinkedQueue<RecordInfo<K, V>> missRecordQueue) {
            this.missRecordQueue = missRecordQueue;
        }

        @Override
        public void run() {
            RecordInfo<K, V> record = this.missRecordQueue.poll();
            if(record != null){
                send(record.topic, record.key, record.value, record.callback);
            }
        }
    }

    private class RecordInfo<K, V>{
        private String topic;
        private K key;
        private V value;
        private Callback callback;

        public RecordInfo(String topic, K key, V value, Callback callback) {
            this.topic = topic;
            this.key = key;
            this.value = value;
            this.callback = callback;
        }
    }
}
