package com.expedia.www.haystack.writer.task;

import com.codahale.metrics.MetricRegistry;
import com.expedia.www.haystack.writer.config.AppConfiguration;
import com.expedia.www.haystack.writer.resources.HealthCheck;
import io.dropwizard.lifecycle.Managed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TaskManager implements Managed {

    private final AppConfiguration config;
    private final TaskStateListener taskStateListener;
    private final MetricRegistry registry;
    private ExecutorService executorService;
    private final List<Task> tasks;

    public TaskManager(final AppConfiguration config, final TaskStateListener taskStateListener, final MetricRegistry registry) {
            this.config = config;
            this.taskStateListener = taskStateListener;
            this.tasks = new ArrayList<>();
            this.registry = registry;
    }

    @Override
    public void start() throws Exception {
        executorService = Executors.newFixedThreadPool(config.getKafka().getThreads());
        for(int taskId = 0; taskId < this.config.getKafka().getThreads(); taskId++) {
            final Task task = new Task(taskId, config, registry);
            task.setStateListener(taskStateListener);
            this.tasks.add(task);
            executorService.submit(task);
        }
    }

    @Override
    public void stop() throws Exception {
        for (final Task task : tasks) {
            task.close();
        }
        executorService.shutdown();
    }
}
