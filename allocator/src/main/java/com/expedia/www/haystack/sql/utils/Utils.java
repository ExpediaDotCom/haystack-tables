package com.expedia.www.haystack.sql.utils;

import com.expedia.www.haystack.sql.entities.QueryRequest;

import java.util.List;
import java.util.stream.Collectors;

public abstract class Utils {

    private static MurmurHash hash = MurmurHash.createRepeatableHasher();
    public static String appId(final QueryRequest query) {
        final List<String> elements = query.getSelect().stream().sorted().collect(Collectors.toList());
        final List<String> conditions = query.getWhere().entrySet()
                .stream().map(e -> e.getKey() + "=" + e.getValue())
                .sorted()
                .collect(Collectors.toList());
        elements.add("where");
        elements.addAll(conditions);

        // concat all records in the list and compute its hash
        final long hashCode = hash.hashToLong(elements.stream().reduce("", (s1, s2) -> s1 + " " + s2).getBytes());
        return String.valueOf(Math.abs(hashCode));
    }
}
