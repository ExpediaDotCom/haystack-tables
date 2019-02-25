package com.expedia.www.haystack.writer.uploader;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

public interface Uploader extends Closeable {
    void upload(final File file, final String destFullPath) throws IOException;
}
