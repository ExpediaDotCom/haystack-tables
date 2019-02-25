package com.expedia.www.haystack.writer.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class S3Configuration {
    private String bucket;
    private String region;
}
