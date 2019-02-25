package com.expedia.www.haystack.sql.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@ToString
public class ExecutorConfig {

    @JsonProperty
    @Getter @Setter
    private String name;

    @JsonProperty
    @NotNull
    @Setter
    private Map<String, String> props = Collections.emptyMap();


    public Map<String, String> getProps() {
        final Map<String, String> additionalProps = new HashMap<>(this.props);
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("HAYSTACK_PROP_EXECUTOR_PROPS_")) {
                final String normalizedKey = key
                        .replace("HAYSTACK_PROP_EXECUTOR_PROPS_", "")
                        .replace("_", ".").toLowerCase();
                additionalProps.put(normalizedKey, value);
            }
        });

        return additionalProps;
    }
}
