package com.expedia.www.haystack.sql.executors;

import com.expedia.www.haystack.sql.entities.QueryMetadata;
import com.expedia.www.haystack.sql.entities.QueryResponse;
import com.expedia.www.haystack.table.entities.Query;

import java.util.List;
import java.util.Map;

public interface QueryExecutor {
    QueryResponse execute(Query query) throws Exception;

    List<QueryMetadata> list() throws Exception;

    QueryResponse delete(String viewName) throws Exception;

    String name();

    void init(Map<String, String> properties) throws Exception;
}