package com.expedia.www.haystack.writer.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@ToString
public class Query {
    @JsonProperty
    private List<String> select;

    @JsonProperty
    private Map<String, String> where;
}