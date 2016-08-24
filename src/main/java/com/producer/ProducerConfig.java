package com.producer;

import org.apache.kafka.common.config.types.Password;

import java.util.List;
import java.util.Properties;

/**
 * @author 谢俊权
 * @create 2016/8/22 10:21
 */
public class ProducerConfig {

    private Properties config = new Properties();

    private ProducerConfig(Builder builder){
        putConfig("bootstrap.servers", builder.bootstrapServers);
        putConfig("key.serializer", builder.keySerializer);
        putConfig("value.serializer", builder.valueSerializer);
        putConfig("acks", builder.acks);
        putConfig("buffer.memory", builder.bufferMemory);
        putConfig("compression.type", builder.compressionType);
        putConfig("retries", builder.retries);
        putConfig("ssl.key.password", builder.sslKeyPassword);
        putConfig("ssl.keystore.location", builder.sslKeystoreLocation);
        putConfig("ssl.keystore.password", builder.sslKeystorePassword);
        putConfig("ssl.truststore.location", builder.sslTruststoreLocation);
        putConfig("ssl.truststore.password", builder.sslTruststorePassword);
        putConfig("batch.size", builder.batchSize);
        putConfig("client.id", builder.clientId);
        putConfig("connections.max.idle.ms", builder.connectionsMaxIdleMs);
        putConfig("linger.ms", builder.lingerMs);
        putConfig("max.block.ms", builder.maxBlockMs);
        putConfig("max.request.size", builder.maxRequestSize);
        putConfig("partitioner.class", builder.partitionerClass);
        putConfig("receive.buffer.bytes", builder.receiveBufferBytes);
        putConfig("request.timeout.ms", builder.requestTimeoutMs);
        putConfig("sasl.kerberos.service.name", builder.saslkerberosServiceName);
        putConfig("sasl.mechanism", builder.saslMechanism);
        putConfig("security.protocol", builder.securityProtocol);
        putConfig("send.buffer.bytes", builder.sendBufferBytes);
        putConfig("ssl.enabled.protocols", builder.sslEnabledProtocols);
        putConfig("ssl.keystore.type", builder.sslKeystoreType);
        putConfig("ssl.protocol", builder.sslProtocol);
        putConfig("ssl.provider", builder.sslProvider);
        putConfig("ssl.truststore.type", builder.sslTruststoreType);
        putConfig("timeout.ms", builder.timeoutMs);
        putConfig("block.on.buffer.full", builder.blockOnBufferFull);
        putConfig("interceptor.classes", builder.interceptorClasses);
        putConfig("max.in.flight.requests.per.connection", builder.maxInFlightRequestsPerConnection);
        putConfig("metadata.fetch.timeout.ms", builder.metadataFetchTimeoutMs);
        putConfig("metadata.max.age.ms", builder.metadataMaxAgeMs);
        putConfig("metric.reporters", builder.metricReporters);
        putConfig("metrics.num.samples", builder.metricsNumSamples);
        putConfig("metrics.sample.window.ms", builder.metricsSampleWindowMs);
        putConfig("reconnect.backoff.ms", builder.reconnectBackoffMs);
        putConfig("retry.backoff.ms", builder.retryBackoffMs);
        putConfig("sasl.kerberos.kinit.cmd", builder.saslkerberosKintCmd);
        putConfig("sasl.kerberos.min.time.before.relogin", builder.saslKerberosMinTimeBeforeRelogin);
        putConfig("sasl.kerberos.ticket.renew.jitter", builder.saslKerberosTicketRenewJitter);
        putConfig("sasl.kerberos.ticket.renew.window.factor", builder.saslKerberosTicketRenewWindowFactor);
        putConfig("ssl.cipher.suites", builder.sslCipherSuites);
        putConfig("ssl.endpoint.identification.algorithm", builder.sslEndpointIdentificationAlgorithm);
        putConfig("ssl.keymanager.algorithm", builder.sslKeymanagerAlgorithm);
        putConfig("ssl.trustmanager.algorithm", builder.sslTrustmanagerAlgorithm);
    }

    private void putConfig(String configName, Object configValue){
        if(configName != null && configValue != null) {
            config.put(configName, configValue);
        }
    }

    public Properties get() {
        return config;
    }

    public static class Builder{

        private List<String> bootstrapServers;

        private Class keySerializer;

        private Class valueSerializer;

        private String acks;

        private Long bufferMemory;

        private String compressionType;

        private Integer retries;

        private Password sslKeyPassword;

        private String sslKeystoreLocation;

        private Password sslKeystorePassword;

        private String sslTruststoreLocation;

        private Password sslTruststorePassword;

        private Integer batchSize;

        private String clientId;

        private Long connectionsMaxIdleMs;

        private Long lingerMs;

        private Long maxBlockMs;

        private Integer maxRequestSize;

        private Class partitionerClass;

        private Integer receiveBufferBytes;

        private Integer requestTimeoutMs;

        private String saslkerberosServiceName;

        private String saslMechanism;

        private String securityProtocol;

        private Integer sendBufferBytes;

        private List sslEnabledProtocols;

        private String sslKeystoreType;

        private String sslProtocol;

        private String sslProvider;

        private String sslTruststoreType;

        private Integer timeoutMs;

        private Boolean blockOnBufferFull;

        private List interceptorClasses;

        private Integer maxInFlightRequestsPerConnection;

        private Long metadataFetchTimeoutMs;

        private Long metadataMaxAgeMs;

        private List metricReporters;

        private Integer metricsNumSamples;

        private Long metricsSampleWindowMs;

        private Long reconnectBackoffMs;

        private Long retryBackoffMs;

        private String saslkerberosKintCmd;

        private Long saslKerberosMinTimeBeforeRelogin;

        private Double saslKerberosTicketRenewJitter;

        private Double saslKerberosTicketRenewWindowFactor;

        private List sslCipherSuites;

        private String sslEndpointIdentificationAlgorithm;

        private String sslKeymanagerAlgorithm;

        private String sslTrustmanagerAlgorithm;

        public Builder bootstrapServers(List<String> bootstrapServers){
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder keySerializer(Class keySerializer){
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder valueSerializer(Class valueSerializer){
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder acks(String acks){
            this.acks = acks;
            return this;
        }

        public Builder bufferMemory(Long bufferMemory){
            this.bufferMemory = bufferMemory;
            return this;
        }

        public Builder compressionType(String compressionType){
            this.compressionType = compressionType;
            return this;
        }

        public Builder sslKeystorePassword(Password sslKeystorePassword){
            this.sslKeystorePassword = sslKeystorePassword;
            return this;
        }

        public Builder retries(Integer retries){
            this.retries = retries;
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

        public Builder sslTruststoreLocation(String sslTruststoreLocation){
            this.sslTruststoreLocation = sslTruststoreLocation;
            return this;
        }

        public Builder sslTruststorePassword(Password sslTruststorePassword){
            this.sslTruststorePassword = sslTruststorePassword;
            return this;
        }

        public Builder batchSize(Integer batchSize){
            this.batchSize = batchSize;
            return this;
        }

        public Builder clientId(String clientId){
            this.clientId = clientId;
            return this;
        }

        public Builder connectionsMaxIdleMs(Long connectionsMaxIdleMs){
            this.connectionsMaxIdleMs = connectionsMaxIdleMs;
            return this;
        }

        public Builder lingerMs(Long lingerMs){
            this.lingerMs = lingerMs;
            return this;
        }

        public Builder maxBlockMs(Long maxBlockMs){
            this.maxBlockMs = maxBlockMs;
            return this;
        }

        public Builder maxRequestSize(Integer maxRequestSize){
            this.maxRequestSize = maxRequestSize;
            return this;
        }

        public Builder partitionerClass(Class partitionerClass){
            this.partitionerClass = partitionerClass;
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

        public Builder saslkerberosServiceName(String saslkerberosServiceName){
            this.saslkerberosServiceName = saslkerberosServiceName;
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

        public Builder timeoutMs(Integer timeoutMs){
            this.timeoutMs = timeoutMs;
            return this;
        }

        public Builder blockOnBufferFull(Boolean blockOnBufferFull){
            this.blockOnBufferFull = blockOnBufferFull;
            return this;
        }

        public Builder interceptorClasses(List interceptorClasses){
            this.interceptorClasses = interceptorClasses;
            return this;
        }

        public Builder maxInFlightRequestsPerConnection(Integer maxInFlightRequestsPerConnection){
            this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
            return this;
        }

        public Builder metadataFetchTimeoutMs(Long metadataFetchTimeoutMs){
            this.metadataFetchTimeoutMs = metadataFetchTimeoutMs;
            return this;
        }

        public Builder metadataMaxAgeMs(Long metadataMaxAgeMs){
            this.metadataMaxAgeMs = metadataMaxAgeMs;
            return this;
        }

        public Builder metricReporters(List metricReporters){
            this.metricReporters = metricReporters;
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

        public Builder saslkerberosKintCmd(String saslkerberosKintCmd){
            this.saslkerberosKintCmd = saslkerberosKintCmd;
            return this;
        }

        public Builder saslKerberosMinTimeBeforeRelogin(Long saslKerberosMinTimeBeforeRelogin){
            this.saslKerberosMinTimeBeforeRelogin = saslKerberosMinTimeBeforeRelogin;
            return this;
        }

        public Builder saslKerberosTicketRenewJitter(Double saslKerberosTicketRenewJitter){
            this.saslKerberosTicketRenewJitter = saslKerberosTicketRenewJitter;
            return this;
        }

        public Builder saslKerberosTicketRenewWindowFactor(Double saslKerberosTicketRenewWindowFactor){
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

        public ProducerConfig build(){
            return new ProducerConfig(this);
        }

    }
}
