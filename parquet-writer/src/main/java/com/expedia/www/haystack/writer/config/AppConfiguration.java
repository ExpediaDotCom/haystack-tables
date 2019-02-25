package com.expedia.www.haystack.writer.config;

import com.expedia.www.haystack.writer.uploader.S3Uploader;
import com.expedia.www.haystack.writer.uploader.Uploader;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Closeable;
import java.io.IOException;

@ToString(callSuper=true)
public class AppConfiguration extends Configuration implements Closeable {

    @JsonProperty
    @Getter @Setter KafkaConfiguration kafka;

    @JsonProperty
    @Getter @Setter
    SqlQueryConfiguration sql;

    @JsonProperty
    @Getter @Setter S3Configuration s3;

    @JsonIgnore
    public Uploader getUploader() {
        return new S3Uploader(s3);
    }

    @Override
    public void close() throws IOException {
        if(this.getUploader() != null) {
            this.getUploader().close();
        }
    }
}
