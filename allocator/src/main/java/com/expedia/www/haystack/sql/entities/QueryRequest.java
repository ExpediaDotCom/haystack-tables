package com.expedia.www.haystack.sql.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class QueryRequest {
    private List<String> select;

    @JsonProperty
    private Map<String, String> where;
}
