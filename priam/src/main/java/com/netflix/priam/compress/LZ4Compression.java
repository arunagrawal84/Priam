/*
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.netflix.priam.compress;

import net.jpountz.lz4.LZ4BlockInputStream;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Iterator;

/**
 * Created by aagrawal on 6/28/17.
 */
public class LZ4Compression implements ICompression {
    private static final int BUFFER = 2 * 1024;

    public CompressionType getCompressionType() {
       return CompressionType.FILE_LEVEL_LZ4;
    }

    @Override
    public void decompressAndClose(InputStream input, OutputStream output) throws IOException {
        byte data[] = new byte[BUFFER];
        try(
                LZ4BlockInputStream inputStream = new LZ4BlockInputStream(input);
        )
        {
            int len;
            while ((len = inputStream.read(data)) > 0)
            {
                output.write(data, 0, len);
            }
        }
        finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(input);
        }
    }

    @Override
    public Iterator<byte[]> compress(InputStream is, long chunkSize) throws IOException {
        return new ChunkedStream(is, chunkSize, CompressionType.FILE_LEVEL_LZ4);
    }
}
