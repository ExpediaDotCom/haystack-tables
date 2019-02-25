package com.expedia.www.haystack.sql.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString(callSuper=true)
public class AppConfiguration extends Configuration {
    @JsonProperty
    @Getter @Setter private ExecutorConfig executor;

    @JsonProperty
    @Getter @Setter private AthenaConfiguration athena;
}
