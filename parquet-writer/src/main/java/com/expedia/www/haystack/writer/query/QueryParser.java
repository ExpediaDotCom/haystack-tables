package com.expedia.www.haystack.writer.query;

import lombok.Getter;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.Validate;

import java.util.HashSet;
import java.util.Set;

public class QueryParser {

    @Getter private final Set<String> selectTags;
    @Getter private boolean selectOperation;
    @Getter private boolean selectDuration;

    @Getter private String serviceNameCond;
    @Getter private String operationNameCond;

    @Getter private Schema schema;

    public QueryParser(final Query query) {
        this.selectTags = new HashSet<>();

        //handle the select
        for (final String field : query.getSelect()) {
            if (field.startsWith("tags[")) {
                selectTags.add(field.replace("tags[", "").replace("]", "").toLowerCase());
            } else if (field.equalsIgnoreCase("operationname")) {
                this.selectOperation = true;
            } else if (field.equalsIgnoreCase("duration")) {
                this.selectDuration = true;
            }
        }

        // handle the where clause
        query.getWhere().forEach((key, value) -> {
            if(key.equalsIgnoreCase("servicename")) {
                serviceNameCond = value;
            } else if(key.equalsIgnoreCase("operationname")) {
                operationNameCond = value;
            } else {
                throw new RuntimeException(String.format("Fail to parse the query with unknown where clause - %s", query));
            }
        });

        Validate.notNull(serviceNameCond);

        this.schema = createSchema();
    }

    private Schema createSchema() {
        final SchemaBuilder.FieldAssembler<Schema> assembler = SchemaBuilder
                .record("SpanSlice")
                .namespace("com.expedia.www.span")
                .fields()
                .name("starttime").type().nullable().longType().noDefault()
                .name("servicename").type().nullable().stringType().noDefault();

        if (selectDuration) {
            assembler.name("duration").type().nullable().longType().noDefault();
        }

        if(selectOperation || operationNameCond != null) {
            assembler.name("operationname").type().nullable().stringType().noDefault();
        }

        selectTags.forEach(tag -> assembler.name(tag).type().nullable().stringType().noDefault());
        return assembler.endRecord();
    }
}


//select duration, tags[errorcode], tags[lob], operationname where servicename=oms
