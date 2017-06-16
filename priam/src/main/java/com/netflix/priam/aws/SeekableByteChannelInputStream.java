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

package com.netflix.priam.aws;
//
//import com.google.common.base.Throwables;
//import sun.nio.ch.ChannelInputStream;
//import java.nio.channels.SeekableByteChannel;
//
//import java.io.IOException;
//
///**
// * Created by aagrawal on 6/2/17.
// */
//public class SeekableByteChannelInputStream extends ChannelInputStream {
//    public final SeekableByteChannel ch;
//    public long markPos = -1;
//
//    public SeekableByteChannelInputStream(final SeekableByteChannel channel) {
//        super(channel);
//        this.ch = channel;
//    }
//
//    @Override
//    public long skip(final long n) throws IOException {
//        final long position = Math.max(0, Math.min(ch.size(), ch.position() + n));
//        final long skipped = Math.abs(position - ch.position());
//
//        ch.position(position);
//
//        return skipped;
//    }
//
//
//    @Override
//    public synchronized void mark(final int readlimit) {
//        try {
//            markPos = ch.position();
//
//        } catch (IOException e) {
//            throw Throwables.propagate(e);
//        }
//    }
//
//    @Override
//    public synchronized void reset() throws IOException {
//        if (markPos < 0)
//            throw new IOException("Resetting to invalid mark");
//
//        ch.position(markPos);
//    }
//
//    @Override
//    public boolean markSupported() {
//        return true;
//    }
//}
