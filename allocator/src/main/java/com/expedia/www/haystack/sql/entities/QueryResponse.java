package com.expedia.www.haystack.sql.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class QueryResponse {
    String viewName;

    String message;

    @JsonIgnore
    int httpStatusCode;
}
