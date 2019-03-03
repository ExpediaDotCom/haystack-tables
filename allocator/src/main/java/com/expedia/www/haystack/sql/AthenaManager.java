package com.expedia.www.haystack.sql;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.athena.model.QueryExecutionContext;
import com.amazonaws.services.athena.model.ResultConfiguration;
import com.amazonaws.services.athena.model.StartQueryExecutionRequest;
import com.amazonaws.util.IOUtils;
import com.expedia.www.haystack.sql.config.AthenaConfiguration;
import com.expedia.www.haystack.sql.entities.QueryMetadata;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class AthenaManager {
    private final AmazonAthena athena;
    private AthenaConfiguration config;
    private String createTableTemplate;
    private String repairTableTemplate;

    public AthenaManager(final AthenaConfiguration config) throws IOException {
        this.config = config;
        this.athena = AmazonAthenaClientBuilder.standard()
                .withRegion(config.getRegion())
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withClientConfiguration(new ClientConfiguration().withClientExecutionTimeout(2 * 60 * 1000)).build();

        this.createTableTemplate = IOUtils.toString(this.getClass().getResourceAsStream("/createTable.sql"));
        this.repairTableTemplate = IOUtils.toString(this.getClass().getResourceAsStream("/repairTable.sql"));
    }


    public void refresh(final QueryMetadata query) {
        final String table = String.format("%s.%s", config.getDatabase(), query.getQuery().getView());
        creatTable(query, table);
        repairTable(table);
        log.info("refreshing athena for query {}", query);

    }

    private void repairTable(final String tableName) {
        final String sql = String.format(repairTableTemplate, tableName);
        executeQuery(sql);
    }

    private void creatTable(final QueryMetadata query, final String tableName) {
        final String sql = String.format(createTableTemplate, tableName, formTableColumns(query), this.config.getBucket(), query.getQuery().getView());
        executeQuery(sql);
    }

    private void executeQuery(final String sql) {
        final QueryExecutionContext context = new QueryExecutionContext().withDatabase(this.config.getDatabase());

        ResultConfiguration resultConfiguration = new ResultConfiguration()
                .withOutputLocation("s3://" + this.config.getBucket() + "/" + this.config.getPrefix() + "/");
        final StartQueryExecutionRequest request = new StartQueryExecutionRequest()
                .withQueryString(sql)
                .withQueryExecutionContext(context)
                .withResultConfiguration(resultConfiguration);

        try {
            athena.startQueryExecution(request);
        } catch (Exception ex) {
            log.error("Fail to execute the query {}", sql, ex);
            throw new RuntimeException(ex);
        }
    }

    private String formTableColumns(QueryMetadata query) {
        //handle the select
        final StringBuilder builder = new StringBuilder();
        for (final String field : query.getQuery().getSelect()) {
            if (field.startsWith("tags[")) {
                builder.append(field.replace("tags[", "").replace("]", "").toLowerCase()).append(" string,");
            } else if (field.equalsIgnoreCase("operationname")) {
                builder.append("operationname string,");
            } else if (field.equalsIgnoreCase("duration")) {
                builder.append("duration int,");
            }
        }

        builder.append("starttime bigint,")
                .append("servicename string");
        return builder.toString();
    }
}
