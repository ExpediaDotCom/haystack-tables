package com.expedia.www.haystack.sql.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
public class QueryRequest {
    @JsonProperty
    @NotEmpty
    private String view;

    @JsonProperty
    @NotNull
    private List<String> select;

    @JsonProperty
    private Map<String, String> where;
}
