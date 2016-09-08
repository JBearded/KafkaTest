package com.tools;

import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;

import java.util.List;

/**
 * @author 谢俊权
 * @create 2016/9/8 10:57
 */
public class TopicTool {

    private ZkUtils zkUtils;

    private static class TopicToolHolder{
        private static TopicTool topicTool = new TopicTool();
    }

    private TopicTool(){
    }

    public static TopicTool getInstance(){
        return TopicToolHolder.topicTool;
    }

    public void init(String zkHosts, int sessionTimeoutMs, int connectionTimeoutMs) {
        boolean isSecureKafkaCluster = false;
        ZkClient zkClient = new ZkClient(zkHosts, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
        zkUtils = new ZkUtils(zkClient, new ZkConnection(zkHosts, sessionTimeoutMs), isSecureKafkaCluster);
    }

    public void create(String topic, int partitions , int replicationFactor){
        String[] options = new String[]{
                "--if-not-exists",
                "--topic",
                topic,
                "--partitions",
                String.valueOf(partitions),
                "--replication-factor",
                String.valueOf(replicationFactor)
        };
        TopicCommand.createTopic(zkUtils, new TopicCommand.TopicCommandOptions(options));
    }

    public void delete(String topic){
        String[] options = new String[]{
                "--if-exists",
                "--topic",
                topic
        };
        TopicCommand.deleteTopic(zkUtils, new TopicCommand.TopicCommandOptions(options));
    }

    public void update(String topic, int partitions , int replicationFactor){
        String[] options = new String[]{
                "--if-exists",
                "--topic",
                topic,
                "--partitions",
                String.valueOf(partitions),
                "--replication-factor",
                String.valueOf(replicationFactor)
        };
        TopicCommand.alterTopic(zkUtils, new TopicCommand.TopicCommandOptions(options));
    }

    public List<MetadataResponse.PartitionMetadata> get(String topic){
        MetadataResponse.TopicMetadata responseTopicMetadata = AdminUtils.fetchTopicMetadataFromZk(topic, zkUtils);
        if(Errors.NONE == responseTopicMetadata.error()){
            return responseTopicMetadata.partitionMetadata();
        }else{
            throw new RuntimeException(responseTopicMetadata.error().exception());
        }
    }
}
