package com.consumer;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.List;
import java.util.Properties;

/**
 * @author 谢俊权
 * @create 2016/8/22 10:21
 */
public class ConsumerConfig {

    private Properties config = new Properties();

    private ConsumerConfig(Builder builder){
        putConfig("bootstrap.servers", builder.bootstrapServers);
        putConfig("key.deserializer", builder.keyDeserializer);
        putConfig("value.deserializer", builder.valueDeserializer);
        putConfig("fetch.min.bytes", builder.fetchMinBytes);
        putConfig("group.id", builder.groupId);
        putConfig("heartbeat.interval.ms", builder.heartbeatIntervalMs);
        putConfig("max.partition.fetch.bytes", builder.maxPartitionFetchBytes);
        putConfig("session.timeout.ms", builder.sessionTimeoutMs);
        putConfig("ssl.key.password", builder.sslKeyPassword);
        putConfig("ssl.keystore.location", builder.sslKeystoreLocation);
        putConfig("ssl.keystore.password", builder.sslKeystorePassword);
        putConfig("ssl.truststore.location", builder.sslTruststoreLocation);
        putConfig("ssl.truststore.password", builder.sslTruststorePassword);
        putConfig("auto.offset.reset", builder.autoOffsetReset);
        putConfig("connections.max.idle.ms",builder.connectionMaxIdleMs);
        putConfig("enable.auto.commit", builder.enableAutoCommit);
        putConfig("exclude.internal.topics", builder.excludeInternalTopics);
        putConfig("max.poll.records", builder.maxPollRecords);
        putConfig("partition.assignment.strategy", builder.partitionAssignmentStrategy);
        putConfig("receive.buffer.bytes", builder.receiveBufferBytes);
        putConfig("request.timeout.ms", builder.requestTimeoutMs);
        putConfig("sasl.kerberos.service.name", builder.saslKerberosServiceName);
        putConfig("sasl.mechanism", builder.saslMechanism);
        putConfig("security.protocol", builder.securityProtocol);
        putConfig("send.buffer.bytes", builder.sendBufferBytes);
        putConfig("ssl.enabled.protocols", builder.sslEnabledProtocols);
        putConfig("ssl.keystore.type", builder.sslKeystoreType);
        putConfig("ssl.protocol", builder.sslProtocol);
        putConfig("ssl.provider", builder.sslProvider);
        putConfig("ssl.truststore.type", builder.sslTruststoreType);
        putConfig("auto.commit.interval.ms", builder.autoCommitIntervalMs);
        putConfig("check.crcs", builder.checkCrcs);
        putConfig("client.id", builder.clientId);
        putConfig("fetch.max.wait.ms", builder.fetchMaxWaitMs);
        putConfig("interceptor.classes", builder.interceptorClasses);
        putConfig("metadata.max.age.ms", builder.metadataMaxAgeMs);
        putConfig("metric.reporters", builder.metricsReporters);
        putConfig("metrics.num.samples", builder.metricsNumSamples);
        putConfig("metrics.sample.window.ms", builder.metricsSampleWindowMs);
        putConfig("reconnect.backoff.ms", builder.reconnectBackoffMs);
        putConfig("retry.backoff.ms", builder.retryBackoffMs);
        putConfig("sasl.kerberos.kinit.cmd", builder.saslKerberosKintCmd);
        putConfig("sasl.kerberos.min.time.before.relogin", builder.saslKerberosMinTimeBeforeRelogin);
        putConfig("sasl.kerberos.ticket.renew.jitter", builder.saslKerberosTicketRenewJitter);
        putConfig("sasl.kerberos.ticket.renew.window.factor", builder.saslKerberosTicketRenewWindowFactor);
        putConfig("ssl.cipher.suites", builder.sslCipherSuites);
        putConfig("ssl.endpoint.identification.algorithm", builder.sslEndpointIdentificationAlgorithm);
        putConfig("ssl.keymanager.algorithm", builder.sslKeymanagerAlgorithm);
        putConfig("ssl.trustmanager.algorithm", builder.sslTrustmanagerAlgorithm);

    }

    private void putConfig(String key, Object value){
        if(key != null && value != null){
            this.config.put(key, value);
        }
    }

    public Properties get() {
        return config;
    }

    public static class Builder{

        private List<String> bootstrapServers;

        private Class<? extends Deserializer> keyDeserializer;

        private Class<? extends Deserializer> valueDeserializer;

        private Integer fetchMinBytes;

        private String groupId;

        private Integer heartbeatIntervalMs;

        private Integer maxPartitionFetchBytes;

        private Integer sessionTimeoutMs;

        private Password sslKeyPassword;

        private String sslKeystoreLocation;

        private Password sslKeystorePassword;

        private String sslTruststoreLocation;

        private Password sslTruststorePassword;

        private String autoOffsetReset;

        private Long connectionMaxIdleMs;

        private Boolean enableAutoCommit;

        private Boolean excludeInternalTopics;

        private Integer maxPollRecords;

        private List partitionAssignmentStrategy;

        private Integer receiveBufferBytes;

        private Integer requestTimeoutMs;

        private String saslKerberosServiceName;

        private String saslMechanism;

        private String securityProtocol;

        private Integer sendBufferBytes;

        private List sslEnabledProtocols;

        private String sslKeystoreType;

        private String sslProtocol;

        private String sslProvider;

        private String sslTruststoreType;

        private Long autoCommitIntervalMs;

        private Boolean checkCrcs;

        private String clientId;

        private Integer fetchMaxWaitMs;

        private List<? extends ConsumerInterceptor> interceptorClasses;

        private Long metadataMaxAgeMs;

        private List<? extends MetricsReporter> metricsReporters;

        private Integer metricsNumSamples;

        private Long metricsSampleWindowMs;

        private Long reconnectBackoffMs;

        private Long retryBackoffMs;

        private String saslKerberosKintCmd;

        private Long saslKerberosMinTimeBeforeRelogin;

        private Double saslKerberosTicketRenewJitter;

        private Double saslKerberosTicketRenewWindowFactor;

        private List sslCipherSuites;

        private String sslEndpointIdentificationAlgorithm;

        private String sslKeymanagerAlgorithm;

        private String sslTrustmanagerAlgorithm;


        public Builder bootstrapServers(List bootstrapServers){
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder keyDeserializer(Class<? extends Deserializer> keyDeserializer){
            this.keyDeserializer = keyDeserializer;
            return this;
        }

        public Builder valueDeserializer(Class<? extends Deserializer> valueDeserializer){
            this.valueDeserializer = valueDeserializer;
            return this;
        }

        public Builder fetchMinBytes(Integer fetchMinBytes){
            this.fetchMinBytes = fetchMinBytes;
            return this;
        }

        public Builder groupId(String groupId){
            this.groupId = groupId;
            return this;
        }

        public Builder heartbeatIntervalMs(Integer heartbeatIntervalMs){
            this.heartbeatIntervalMs = heartbeatIntervalMs;
            return this;
        }

        public Builder maxPartitionFetchBytes(Integer maxPartitionFetchBytes){
            this.maxPartitionFetchBytes = maxPartitionFetchBytes;
            return this;
        }

        public Builder sessionTimeoutMs(Integer sessionTimeoutMs){
            this.sessionTimeoutMs = sessionTimeoutMs;
            return this;
        }

        public Builder sslKeyPassword(Password sslKeyPassword){
            this.sslKeyPassword = sslKeyPassword;
            return this;
        }

        public Builder sslKeystoreLocation(String sslKeystoreLocation){
            this.sslKeystoreLocation = sslKeystoreLocation;
            return this;
        }

        public Builder sslKeystorePassword(Password sslKeystorePassword){
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        public Builder sslTruststoreLocation(String sslTruststoreLocation){
            this.sslTruststoreLocation = sslTruststoreLocation;
            return this;
        }

        public Builder sslTruststorePassword(Password sslTruststorePassword){
            this.sslTruststorePassword = sslTruststorePassword;
            return this;
        }

        public Builder autoOffsetReset(String autoOffsetReset){
            this.autoOffsetReset = autoOffsetReset;
            return this;
        }

        public Builder connectionMaxIdleMs(Long connectionMaxIdleMs){
            this.connectionMaxIdleMs = connectionMaxIdleMs;
            return this;
        }

        public Builder enableAutoCommit(Boolean enableAutoCommit){
            this.enableAutoCommit = enableAutoCommit;
            return this;
        }

        public Builder excludeInternalTopics(Boolean excludeInternalTopics){
            this.excludeInternalTopics = excludeInternalTopics;
            return this;
        }

        public Builder maxPollRecords(Integer maxPollRecords){
            this.maxPollRecords = maxPollRecords;
            return this;
        }

        public Builder partitionAssignmentStrategy(List partitionAssignmentStrategy){
            this.partitionAssignmentStrategy = partitionAssignmentStrategy;
            return this;
        }

        public Builder receiveBufferBytes(Integer receiveBufferBytes){
            this.receiveBufferBytes = receiveBufferBytes;
            return this;
        }

        public Builder requestTimeoutMs(Integer requestTimeoutMs){
            this.requestTimeoutMs = requestTimeoutMs;
            return this;
        }

        public Builder saslKerberosServiceName(String saslKerberosServiceName){
            this.saslKerberosServiceName = saslKerberosServiceName;
            return this;
        }

        public Builder saslMechanism(String saslMechanism){
            this.saslMechanism = saslMechanism;
            return this;
        }

        public Builder securityProtocol(String securityProtocol){
            this.securityProtocol = securityProtocol;
            return this;
        }

        public Builder sendBufferBytes(Integer sendBufferBytes){
            this.sendBufferBytes = sendBufferBytes;
            return this;
        }

        public Builder sslEnabledProtocols(List sslEnabledProtocols){
            this.sslEnabledProtocols = sslEnabledProtocols;
            return this;
        }

        public Builder sslKeystoreType(String sslKeystoreType){
            this.sslKeystoreType = sslKeystoreType;
            return this;
        }

        public Builder sslProtocol(String sslProtocol){
            this.sslProtocol = sslProtocol;
            return this;
        }

        public Builder sslProvider(String sslProvider){
            this.sslProvider = sslProvider;
            return this;
        }

        public Builder sslTruststoreType(String sslTruststoreType){
            this.sslTruststoreType = sslTruststoreType;
            return this;
        }

        public Builder autoCommitIntervalMs(Long autoCommitIntervalMs){
            this.autoCommitIntervalMs = autoCommitIntervalMs;
            return this;
        }

        public Builder checkCrcs(Boolean checkCrcs){
            this.checkCrcs = checkCrcs;
            return this;
        }

        public Builder clientId(String clientId){
            this.clientId = clientId;
            return this;
        }

        public Builder fetchMaxWaitMs(Integer fetchMaxWaitMs){
            this.fetchMaxWaitMs = fetchMaxWaitMs;
            return this;
        }

        public Builder interceptorClasses(List<? extends ConsumerInterceptor> interceptorClasses){
            this.interceptorClasses = interceptorClasses;
            return this;
        }

        public Builder metadataMaxAgeMs(Long metadataMaxAgeMs){
            this.metadataMaxAgeMs = metadataMaxAgeMs;
            return this;
        }

        public Builder metricsReporters(List<? extends MetricsReporter> metricsReporters){
            this.metricsReporters = metricsReporters;
            return this;
        }

        public Builder metricsNumSamples(Integer metricsNumSamples){
            this.metricsNumSamples = metricsNumSamples;
            return this;
        }

        public Builder metricsSampleWindowMs(Long metricsSampleWindowMs){
            this.metricsSampleWindowMs = metricsSampleWindowMs;
            return this;
        }

        public Builder reconnectBackoffMs(Long reconnectBackoffMs){
            this.reconnectBackoffMs = reconnectBackoffMs;
            return this;
        }

        public Builder retryBackoffMs(Long retryBackoffMs){
            this.retryBackoffMs = retryBackoffMs;
            return this;
        }

        public Builder saslKerberosKintCmd(String saslKerberosKintCmd){
            this.saslKerberosKintCmd = saslKerberosKintCmd;
            return this;
        }

        public Builder saslKerberosMinTimeBeforeRelogin(Long  saslKerberosMinTimeBeforeRelogin){
            this.saslKerberosMinTimeBeforeRelogin = saslKerberosMinTimeBeforeRelogin;
            return this;
        }

        public Builder saslKerberosTicketRenewJitter(Double saslKerberosTicketRenewJitter){
            this.saslKerberosTicketRenewJitter = saslKerberosTicketRenewJitter;
            return this;
        }

        public Builder saslKerberosTicketRenewWindowFactor(Double  saslKerberosTicketRenewWindowFactor){
            this.saslKerberosTicketRenewWindowFactor = saslKerberosTicketRenewWindowFactor;
            return this;
        }

        public Builder sslCipherSuites(List sslCipherSuites){
            this.sslCipherSuites = sslCipherSuites;
            return this;
        }

        public Builder sslEndpointIdentificationAlgorithm(String sslEndpointIdentificationAlgorithm){
            this.sslEndpointIdentificationAlgorithm = sslEndpointIdentificationAlgorithm;
            return this;
        }

        public Builder sslKeymanagerAlgorithm(String sslKeymanagerAlgorithm){
            this.sslKeymanagerAlgorithm = sslKeymanagerAlgorithm;
            return this;
        }

        public Builder sslTrustmanagerAlgorithm(String sslTrustmanagerAlgorithm){
            this.sslTrustmanagerAlgorithm = sslTrustmanagerAlgorithm;
            return this;
        }

        public ConsumerConfig build(){
            return new ConsumerConfig(this);
        }

    }
}
