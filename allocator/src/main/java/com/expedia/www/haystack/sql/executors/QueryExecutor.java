package com.expedia.www.haystack.sql.executors;

import com.expedia.www.haystack.sql.entities.QueryMetadata;
import com.expedia.www.haystack.sql.entities.QueryRequest;
import com.expedia.www.haystack.sql.entities.QueryResponse;

import java.util.List;
import java.util.Map;

public interface QueryExecutor {
    QueryResponse execute(QueryRequest query);

    List<QueryMetadata> list() throws Exception;

    QueryResponse delete(String executionId) throws Exception;

    String name();

    void init(Map<String, String> properties) throws Exception;
}