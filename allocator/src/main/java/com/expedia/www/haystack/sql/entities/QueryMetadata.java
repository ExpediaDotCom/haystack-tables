package com.expedia.www.haystack.sql.entities;

import com.expedia.www.haystack.table.entities.Query;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.joda.time.DateTime;

@Data
@NoArgsConstructor
@ToString
public class QueryMetadata {

    @JsonProperty
    private DateTime createTimestamp;

    @JsonProperty
    private DateTime lastUpdatedTimestamp;

    @JsonProperty
    private Query query;

    @JsonProperty
    private boolean running;
}
