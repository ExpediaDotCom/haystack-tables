package com.expedia.www.haystack.sql;

import com.expedia.www.haystack.sql.config.AppConfiguration;
import com.expedia.www.haystack.sql.executors.QueryExecutor;
import com.expedia.www.haystack.sql.resources.HealthCheck;
import com.expedia.www.haystack.sql.resources.SqlQuery;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

import java.util.ServiceLoader;

@Slf4j
public class SqlApplication extends Application<AppConfiguration> {

    public void initialize(Bootstrap<AppConfiguration> bootstrap) {
        bootstrap.getObjectMapper().disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor(false)));
    }

    @Override
    public void run(final AppConfiguration config,
                    final Environment environment) throws Exception {
        log.info("app config {}", config);
        final ClassLoader cl = Thread.currentThread().getContextClassLoader();
        final ServiceLoader<QueryExecutor> executors = ServiceLoader.load(QueryExecutor.class, cl);

        for (final QueryExecutor executor : executors) {
            // find the first executor that matches the name
            if (executor.name().equalsIgnoreCase(config.getExecutor().getName())) {
                //initialize the executor
                executor.init(config.getExecutor().getProps());
                environment.jersey().register(new SqlQuery(executor, new AthenaRefreshJob(config.getAthena())));
                break;
            }
        }

        environment.jersey().register(new HealthCheck());
    }

    public static void main(String[] args) throws Exception {
        new SqlApplication().run("server", args[0]);
    }
}

