package com.expedia.www.haystack.writer.config;


import com.expedia.www.haystack.writer.query.Query;
import com.expedia.www.haystack.writer.query.QueryParser;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.gson.Gson;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.Validate;

@NoArgsConstructor
@ToString
public class SqlQueryConfiguration {

    @Setter
    @JsonProperty
    private String query;

    @JsonIgnore
    public QueryParser getParser() {
        Validate.notEmpty(this.query);
        final Query query = new Gson().fromJson(this.query, Query.class);
        return new QueryParser(query);
    }
}