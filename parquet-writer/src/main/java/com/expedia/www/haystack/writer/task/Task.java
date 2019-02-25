package com.expedia.www.haystack.writer.task;


import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.expedia.open.tracing.Span;
import com.expedia.www.haystack.writer.SpanDeserializer;
import com.expedia.www.haystack.writer.config.AppConfiguration;
import com.expedia.www.haystack.writer.config.KafkaConfiguration;
import com.expedia.www.haystack.writer.config.SqlQueryConfiguration;
import com.expedia.www.haystack.writer.query.QueryParser;
import com.expedia.www.haystack.writer.uploader.Uploader;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Task implements Runnable, Closeable {

    private final int taskId;
    private final KafkaConfiguration cfg;
    private final Uploader uploader;
    private final SqlQueryConfiguration sqlQuery;
    private final QueryParser queryParser;
    private final ScheduledExecutorService wakeupScheduler;
    private final AtomicBoolean shutdownRequested;
    private final KafkaConsumer<String, Span> consumer;
    private int wakeups = 0;
    private final Map<TopicPartition, StreamProcessor> processors;
    private TaskStateListener.State state = TaskStateListener.State.NOT_RUNNING;
    private final List<TaskStateListener> stateListeners;
    private final Histogram lag;

    private class RebalanceListener implements ConsumerRebalanceListener {
        /**
         * close the running processors for the revoked partitions
         *
         * @param revokedPartitions revoked partitions
         */
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
            log.info("Partitions {} revoked at the beginning of consumer rebalance for taskId={}", revokedPartitions, taskId);
            for(final TopicPartition p : revokedPartitions) {
                final StreamProcessor processor = processors.remove(p);
                if (processor != null) {
                    processor.close();
                }
            }
        }

        /**
         * create processors for newly assigned partitions
         *
         * @param assignedPartitions newly assigned partitions
         */
        public void onPartitionsAssigned(Collection<TopicPartition> assignedPartitions) {
            log.info("Partitions {} assigned at the beginning of consumer rebalance for taskId={}", assignedPartitions, taskId);
            for(final TopicPartition p : assignedPartitions) {
                final StreamProcessor newProcessor = new StreamProcessor(queryParser, uploader, sqlQuery.getId(), lag);
                final StreamProcessor previousProcessor = processors.putIfAbsent(p, newProcessor);
                if (previousProcessor == null) {
                    newProcessor.init();
                }
            }
        }
    }

    public Task(final int taskId, final AppConfiguration cfg, final MetricRegistry registry) {
        this.lag = registry.histogram("kafka.iterator.age.ms");

        this.taskId = taskId;
        this.cfg = cfg.getKafka();
        this.sqlQuery = cfg.getSql();
        this.uploader = cfg.getUploader();
        this.stateListeners = new ArrayList<>();
        this.queryParser = cfg.getSql().getParser();
        this.processors  = new ConcurrentHashMap<>();
        this.wakeupScheduler = Executors.newScheduledThreadPool(1);
        this.shutdownRequested = new AtomicBoolean(false);
        this.consumer = createConsumer(cfg);
        consumer.subscribe(Collections.singletonList(cfg.getKafka().getTopic()), new RebalanceListener());
    }

    @Override
    public void run() {
        log.info("Starting stream processing thread with id={}", taskId);
        try {
            updateStateAndNotify(TaskStateListener.State.RUNNING);
            runLoop();
        } catch (InterruptedException ie) {
            log.error("This stream task with taskId={} has been interrupted", taskId, ie);
        } catch (Exception ex) {
            if (!shutdownRequested.get()) updateStateAndNotify(TaskStateListener.State.FAILED);
            // may be logging the exception again for kafka specific exceptions, but it is ok.
            log.error("Stream application faced an exception during processing for taskId={}: ", taskId, ex);
        } finally {
            close();
            updateStateAndNotify(TaskStateListener.State.CLOSED);
        }
    }

    @Override
    public void close() {
        shutdownRequested.set(true);
        consumer.close(Duration.ofMillis(cfg.getCloseTimeoutMillis()));
    }

    public void setStateListener(final TaskStateListener listener) {
        this.stateListeners.add(listener);
    }

    /**
     * run the consumer loop till the shutdown is requested or any exception is thrown
     */
    private void runLoop() throws InterruptedException, IOException {
        while (!shutdownRequested.get()) {
            final Optional<ConsumerRecords<String, Span>> mayBeRecords = poll();
            if (mayBeRecords.isPresent() && !processors.isEmpty()) {
                final Map<Integer, List<ConsumerRecord<String, Span>>> recordsByPartitionMap = new HashMap<>();
                for (ConsumerRecord<String, Span> record : mayBeRecords.get()) {
                    if (record != null) {
                        final List<ConsumerRecord<String, Span>> recordsByPartition = recordsByPartitionMap.computeIfAbsent(record.partition(), k -> new ArrayList<>());
                        recordsByPartition.add(record);
                    }
                }

                final Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>();
                for (Map.Entry<Integer, List<ConsumerRecord<String, Span>>> entry : recordsByPartitionMap.entrySet()) {
                    final TopicPartition tp = new TopicPartition(cfg.getTopic(), entry.getKey());
                    final Optional<OffsetAndMetadata> offset = processors.get(tp).process(entry.getValue());
                    offset.ifPresent(offsetAndMetadata -> committableOffsets.put(tp, offsetAndMetadata));
                }

                if (!committableOffsets.isEmpty()) {
                    consumer.commitSync(committableOffsets);
                }
            }
        }
    }

    private List<TaskStateListener> getStateListeners() {
        return stateListeners;
    }

    private void updateStateAndNotify(final TaskStateListener.State newState) {
        if (state != newState) {
            state = newState;

            // invoke listeners for any state change
            getStateListeners().forEach(listener -> listener.onChange(state));
        }
    }

    /**
     * before requesting consumer.poll(), schedule a wakeup call as poll() may hang due to network errors in kafka
     * if the poll() doesnt return after a timeout, then wakeup the consumer.
     *
     * @return consumer records from kafka
     */
    private Optional<ConsumerRecords<String, Span>> poll() {
        final ScheduledFuture wakeupCall = scheduleWakeup();

        try {
            final ConsumerRecords<String, Span> records = consumer.poll(Duration.ofMillis(cfg.getPollTimeoutMillis()));
            wakeups = 0;
            if (records == null || records.isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(records);
            }
        } catch (WakeupException we) {
            handleWakeupError(we);
            return Optional.empty();
        } finally {
            try {
                wakeupCall.cancel(true);
            } catch (Exception ex) {
                log.error("kafka consumer poll has failed with error", ex);
            }
        }
    }

    private ScheduledFuture scheduleWakeup() {
        return wakeupScheduler.schedule(consumer::wakeup, cfg.getWakeupTimeoutMillis(), TimeUnit.MILLISECONDS);
    }

    private void handleWakeupError(final WakeupException we) {
        if (we == null) {
            return;
        }
        // if in shutdown phase, then do not swallow the exception, throw it to upstream
        if (shutdownRequested.get()) throw we;
        wakeups = wakeups + 1;
        if (wakeups == cfg.getMaxWakeups()) {
            log.error("WakeupException limit exceeded, throwing up wakeup exception for taskId={}.", taskId, we);
            throw we;
        } else {
            log.error("Consumer poll took more than {} ms for taskId={}, wakeup attempt={}!. Will try poll again!",
                    cfg.getWakeupTimeoutMillis(), wakeups, taskId);
        }
    }

    private static KafkaConsumer<String, Span> createConsumer(final AppConfiguration cfg) {
        final Properties consumerProps = cfg.getKafka().getConsumerProps(cfg.getSql().getId());
        return new KafkaConsumer<>(
                consumerProps,
                new StringDeserializer(),
                new SpanDeserializer());
    }
}
