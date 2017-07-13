/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.priam.compress;

import com.netflix.priam.aws.S3FileSystem;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
/**
 * Byte iterator representing compressed data.
 * Uses snappy compression by default
 */
public class ChunkedStream implements Iterator<byte[]>
{
    private static final Logger logger = LoggerFactory.getLogger(ChunkedStream.class);
    private boolean hasnext = true;
    private ByteArrayOutputStream bos;
    private ByteBuffer byteBuffer;
    private OutputStream compress;
    private InputStream origin;
    private long chunkSize;
    private static int BYTES_TO_READ = 2048;
    static final int DEFAULT_SEED = 0x9747b28c;

    public ChunkedStream(InputStream is, long chunkSize) throws IOException
    {
        this(is, chunkSize, CompressionType.FILE_LEVEL_SNAPPY);
    }

    public ChunkedStream(InputStream is, long chunkSize, CompressionType compressionType) throws IOException
    {
        this.origin = is;
        this.chunkSize = chunkSize;
        this.bos = new ByteArrayOutputStream();
        switch (compressionType)
        {
            case FILE_LEVEL_SNAPPY:
                this.compress = new SnappyOutputStream(bos);
                logger.info("Compressing InputStream with compression: {}", compressionType);
                break;
            case FILE_LEVEL_LZ4:
                this.compress = new LZ4BlockOutputStream(bos, 1 << 16, LZ4Factory.fastestInstance().fastCompressor(), XXHashFactory.fastestInstance().newStreamingHash32(DEFAULT_SEED).asChecksum(), true);
                logger.info("Compressing InputStream with compression: {}", compressionType);
                break;
            default:
                throw new IOException("Compression Type: " + compressionType + " is not supported");
        }
    }

    @Override
    public boolean hasNext()
    {
        return hasnext;
    }

    @Override
    public byte[] next()
    {
        try
        {
            byte data[] = new byte[BYTES_TO_READ];
            int count;
            while ((count = origin.read(data, 0, data.length)) != -1)
            {
                compress.write(data, 0, count);
                if (bos.size() >= chunkSize)
                    return returnSafe();
            }
            // We don't have anything else to read hence set to false.
            return done();
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private byte[] done() throws IOException
    {
        compress.flush();
        bos.flush();
        byte[] return_ = bos.toByteArray();
        hasnext = false;
        IOUtils.closeQuietly(compress);
        IOUtils.closeQuietly(bos);
        IOUtils.closeQuietly(origin);
        return return_;
    }

    private byte[] returnSafe() throws IOException
    {
        byte[] return_ = bos.toByteArray();
        bos.reset();
        return return_;
    }

    @Override
    public void remove()
    {
    }

}
