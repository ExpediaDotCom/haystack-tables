package com.expedia.www.haystack.table.entities;

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
    private String view;

    @JsonProperty
    private List<String> select;

    @JsonProperty
    private Map<String, String> where;
}