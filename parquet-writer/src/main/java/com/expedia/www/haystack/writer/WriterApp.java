package com.expedia.www.haystack.writer;

import com.expedia.www.haystack.writer.config.AppConfiguration;
import com.expedia.www.haystack.writer.resources.HealthCheck;
import com.expedia.www.haystack.writer.task.TaskManager;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WriterApp extends Application<AppConfiguration> {

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
        final HealthCheck healthCheck = new HealthCheck();
        environment.lifecycle().manage(new TaskManager(config, healthCheck, environment.metrics()));
        environment.jersey().register(healthCheck);
    }

    public static void main(String[] args) throws Exception {
        new WriterApp().run("server", args[0]);
    }
}

