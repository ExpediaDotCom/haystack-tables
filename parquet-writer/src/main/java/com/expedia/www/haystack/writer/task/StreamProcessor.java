package com.expedia.www.haystack.writer.task;

import com.codahale.metrics.Histogram;
import com.expedia.open.tracing.Span;
import com.expedia.open.tracing.Tag;
import com.expedia.www.haystack.writer.query.QueryParser;
import com.expedia.www.haystack.writer.uploader.Uploader;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class StreamProcessor implements Closeable {
    private final static long MAX_PARQUET_FILE_SIZE = 20L * 1024 * 1024;
    private final static long MAX_FLUSH_INTERVAL_SEC = 60;

    private final QueryParser parser;
    private final Schema schema;
    private final Uploader uploader;
    private final Histogram lag;

    private ParquetWriter<Object> writer;
    private Instant lastUpdatedInstant = Instant.now();
    private long matchedRecords = 0;
    private File file;

    public StreamProcessor(final QueryParser queryParser, final Uploader uploader, final Histogram lag) {
        Validate.notNull(queryParser);
        this.schema = queryParser.getSchema();
        this.parser = queryParser;
        this.uploader = uploader;
        this.lag = lag;
    }

    public void init() {
        try {
            this.writer = newParquetWriter();
        } catch (IOException ex) {
            log.error("Fail to initialize the stream processor", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void close() {
        try {
            this.writer.close();
        } catch (Exception ex) {
            log.error("Fail to close the parquet writer", ex);
        }
    }

    public Optional<OffsetAndMetadata> process(final List<ConsumerRecord<String, Span>> records) throws IOException {
        int partition = -1;
        long lastOffset = -1;
        long timestampMicros = -1;

        for (final ConsumerRecord<String, Span> record : records) {
            final Span span = record.value();
            if (isMatched(span)) {
                this.writer.write(createAvroRecord(span));
                matchedRecords++;
            }
            partition = record.partition();
            lastOffset = record.offset();
            timestampMicros = Math.max(span.getStartTime(), timestampMicros);
        }

        this.lag.update(System.currentTimeMillis() - (timestampMicros/1000));
        if (shouldFlush()) {
            this.writer.close();
            uploader.upload(this.file, createUploadFullPath(timestampMicros/1000, partition, lastOffset));
            cleanup();
            this.writer = newParquetWriter();
            this.lastUpdatedInstant = Instant.now();
            this.matchedRecords = 0;
            return Optional.of(new OffsetAndMetadata(lastOffset));
        }

        return Optional.empty();
    }

    private void cleanup() {
        if (!this.file.delete()) {
            log.error("Fail to delete the parquet file!!!");
        }
        //delete the crc file
        if (!new File("." + this.file.getName() + ".crc").delete()) {
            log.error("Fail to delete the crc file !!");
        }
    }

    private String createUploadFullPath(long timestamp, int partition, long lastOffset) {
        final DateTime dt = new DateTime(timestamp);
        return String.format("sql/%s/year=%d/month=%02d/day=%02d/hour=%02d/%s.parquet",
                parser.getName(),
                dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), dt.getHourOfDay(), partition +"_" + lastOffset);
    }

    private boolean shouldFlush() {
        return matchedRecords > 0
                && this.writer.getDataSize() > MAX_PARQUET_FILE_SIZE
                || Instant.now().minusSeconds(MAX_FLUSH_INTERVAL_SEC).isAfter(lastUpdatedInstant);
    }

    private GenericData.Record createAvroRecord(final Span span) {
        final GenericData.Record avroRecord = new GenericData.Record(schema);
        avroRecord.put("servicename", span.getServiceName());
        avroRecord.put("starttime", span.getStartTime());

        if (parser.isSelectDuration()) {
            avroRecord.put("duration", span.getDuration());
        }

        if (parser.getOperationNameCond() != null || parser.isSelectOperation()) {
            avroRecord.put("operationname", span.getOperationName());
        }

        for (final Tag tag : span.getTagsList()) {
            final String normalizedKey = tag.getKey().toLowerCase();
            if (parser.getSelectTags().contains(normalizedKey)) {
                avroRecord.put(normalizedKey, tag.getVStr());
            }
        }
        return avroRecord;
    }

    private boolean isMatched(final Span span) {
        return span.getServiceName().equalsIgnoreCase(parser.getServiceNameCond())
                && (parser.getOperationNameCond() == null
                || span.getOperationName().equalsIgnoreCase(parser.getOperationNameCond()));
    }

    private ParquetWriter<Object> newParquetWriter() throws IOException {
        this.file = new File(UUID.randomUUID().toString());
        return AvroParquetWriter.builder(new Path(file.getPath()))
                .withPageSize(64 * 1024)
                .withRowGroupSize(64 * 1024 * 1024)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }
}
