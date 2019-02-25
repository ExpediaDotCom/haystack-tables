package com.expedia.www.haystack.writer.uploader;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.expedia.www.haystack.writer.config.S3Configuration;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

@Slf4j
public class S3Uploader implements Uploader {

    private final AmazonS3 s3;
    private final String bucketName;

    public S3Uploader(final S3Configuration config) {
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.fromName(config.getRegion())).build();
        this.bucketName = config.getBucket();
    }

    @Override
    public void upload(final File file, final String destFullPath) throws IOException {
        try {
            s3.putObject(bucketName, destFullPath, file);
            log.info("successfully upload the parquet file at {}", destFullPath);
        } catch (Exception ex) {
            log.error("Fail to upload the file to s3 at dest path:" + destFullPath, ex);
        }
    }

    @Override
    public void close() throws IOException {
        s3.shutdown();
    }
}
