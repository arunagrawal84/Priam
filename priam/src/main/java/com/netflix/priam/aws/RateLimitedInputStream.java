/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.priam.aws;

/**
 * Created by aagrawal on 5/9/17.
 */

import com.amazonaws.internal.ResettableInputStream;
import com.google.common.util.concurrent.RateLimiter;

import java.io.IOException;
import java.io.InputStream;


public class RateLimitedInputStream extends InputStream {
    private final InputStream inputStream;
    final RateLimiter limiter;

    public RateLimitedInputStream(final InputStream inputStream, final RateLimiter limiter) {
        this.inputStream = inputStream;
        this.limiter = limiter;
    }

    @Override
    public int read() throws IOException {
        limiter.acquire();
        return inputStream.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        limiter.acquire(b.length);
        return inputStream.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        limiter.acquire(len);
        return inputStream.read(b, off, len);
    }

    @Override
    public boolean markSupported() {
        return inputStream.markSupported();
    }

    @Override
    public int available() throws IOException {
        return inputStream.available();
    }

    @Override
    public void close() throws IOException {
        inputStream.close();
    }

    @Override
    public synchronized void mark(final int readlimit) {
        inputStream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        inputStream.reset();
    }

}
