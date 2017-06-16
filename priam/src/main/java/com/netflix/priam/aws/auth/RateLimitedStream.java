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

package com.netflix.priam.aws.auth;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SelectableChannel;

/**
 * Created by aagrawal on 6/5/17.
 */
public class RateLimitedStream extends InputStream {
    final RateLimiter limiter;
    public final SeekableByteChannel channel;
    public long markPos = -1;
    private byte[] bytesPrevious = null;           // Invoker's previous array
    private byte[] b1 = null;
    private ByteBuffer byteBuffer = null;


    public RateLimitedStream(final SeekableByteChannel channel, final RateLimiter limiter) {
        this.channel = channel;
        this.limiter = limiter;
    }

    @Override
    public int read() throws IOException {
        limiter.acquire();
        if (b1 == null)
            b1 = new byte[1];
        int n = this.read(b1);
        if (n == 1)
            return b1[0] & 0xff;
        return -1;
    }

    @Override
    public int read(final byte[] bytes) throws IOException {
        return read(bytes, 0, bytes.length);
    }

    public static int read(SeekableByteChannel channel, ByteBuffer byteBuffer,
                           boolean block)
            throws IOException
    {
        if (channel instanceof SelectableChannel) {
            SelectableChannel sc = (SelectableChannel)channel;
            synchronized (sc.blockingLock()) {
                boolean blockingMode = sc.isBlocking();
                if (!blockingMode)
                    throw new IllegalBlockingModeException();
                if (blockingMode != block)
                    sc.configureBlocking(block);
                int n = channel.read(byteBuffer);
                if (blockingMode != block)
                    sc.configureBlocking(blockingMode);
                return n;
            }
        } else {
            return channel.read(byteBuffer);
        }
    }

    @Override
    public int read(final byte[] byteArray, final int off, final int len) throws IOException {
        if ((off < 0) || (off > byteArray.length) || (len < 0) ||
                ((off + len) > byteArray.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0)
            return 0;

        limiter.acquire(len);

        ByteBuffer byteBuffer = ((this.bytesPrevious == byteArray)
                ? this.byteBuffer
                : ByteBuffer.wrap(byteArray));
        byteBuffer.limit(Math.min(off + len, byteBuffer.capacity()));
        byteBuffer.position(off);
        this.byteBuffer = byteBuffer;
        this.bytesPrevious = byteArray;
        return read(byteBuffer);
    }

    protected int read(ByteBuffer bb)
            throws IOException
    {
        return RateLimitedStream.read(channel, bb, true);
    }


    @Override
    public int available() throws IOException {
        // special case where the channel is to a file
        if (channel instanceof SeekableByteChannel) {
            SeekableByteChannel seekableByteChannel = (SeekableByteChannel) channel;
            long rem = Math.max(0, seekableByteChannel.size() - seekableByteChannel.position());
            return (rem > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)rem;
        }
        return 0;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public long skip(final long n) throws IOException {
        final long position = Math.max(0, Math.min(channel.size(), channel.position() + n));
        final long skipped = Math.abs(position - channel.position());

        channel.position(position);

        return skipped;
    }


    @Override
    public synchronized void mark(final int readlimit) {
        try {
            markPos = channel.position();

        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        if (markPos < 0)
            throw new IOException("Resetting to invalid mark");

        channel.position(markPos);
    }

    @Override
    public boolean markSupported() {
        return true;
    }

}
