package com.expedia.www.haystack.sql.utils;

import com.expedia.www.haystack.sql.entities.QueryRequest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class Utils {
    public static boolean isSubset(final QueryRequest requested, final QueryRequest running) {
        final List<String> runningSelectFields =
                running.getSelect().stream().map(String::toLowerCase).collect(Collectors.toList());

        // all fields in requested query should be a subset of the running query's fields
        final boolean selectFieldsPresent = requested.getSelect().stream().allMatch(f -> runningSelectFields.contains(f.toLowerCase()));
        if (!selectFieldsPresent) {
            return false;
        }

        // check if 'where' clause of the requested query is more restrictive than running query
        // if yes, then dont create a new one
        for (Map.Entry<String, String> clause : running.getWhere().entrySet()) {
            final String whereVal = requested.getWhere().get(clause.getKey());
            if (whereVal == null || !whereVal.equalsIgnoreCase(clause.getValue().toLowerCase())) {
                    return false;
            }
        }
        return true;
    }
}
