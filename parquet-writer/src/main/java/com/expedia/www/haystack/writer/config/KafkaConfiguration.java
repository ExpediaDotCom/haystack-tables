package com.expedia.www.haystack.writer.config;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;
import java.util.Properties;

@ToString
public class KafkaConfiguration {
    private final static String HaystackConsumerEnvPrefix = "HAYSTACK_PROP_KAFKA_CONSUMER_";

    @JsonProperty
    @Getter
    @Setter
    private String topic;

    @JsonProperty
    @Getter
    @Setter
    private int threads;

    @JsonProperty
    @Getter
    @Setter
    private int closeTimeoutMillis;

    @JsonProperty
    @Getter
    @Setter
    private int maxWakeups;

    @JsonProperty
    @Getter
    @Setter
    private int wakeupTimeoutMillis;

    @JsonProperty
    @Getter
    @Setter
    private int pollTimeoutMillis;

    @JsonProperty
    @Setter
    private Map<String, String> consumer;

    public Properties getConsumerProps(final String queryName) {
        final Properties props = new Properties();
        consumer.forEach(props::setProperty);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "haystack-table-parquet-writer-" + queryName);
        final Map<String, String> env = System.getenv();
        env.forEach((key, value) -> {
            if (key.startsWith(HaystackConsumerEnvPrefix)) {
                props.setProperty(key
                        .replaceFirst(HaystackConsumerEnvPrefix, "")
                        .replaceAll("_", ".")
                        .toLowerCase(), value);
            }
        });
        return props;
    }
}
