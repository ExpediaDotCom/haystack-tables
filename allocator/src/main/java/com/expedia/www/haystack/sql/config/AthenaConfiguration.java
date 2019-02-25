package com.expedia.www.haystack.sql.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class AthenaConfiguration {

    @Getter
    @Setter
    @JsonProperty
    private String bucket;

    @Getter
    @Setter
    @JsonProperty
    private String prefix;

    @Getter
    @Setter
    @JsonProperty
    private String region;

    @Getter
    @Setter
    @JsonProperty
    private String database;
}
