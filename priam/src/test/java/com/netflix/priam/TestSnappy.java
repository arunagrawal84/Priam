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

package com.netflix.priam;

import com.netflix.priam.compress.*;
import net.jpountz.lz4.LZ4BlockOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by aagrawal on 6/23/17.
 */
public class TestSnappy {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSnappy.class);
    public static final String DATA_DIR = "/Users/aagrawal/workspace/testfiles";
    public static ICompression snappyCompression = new SnappyCompression();
    public static ICompression lz4Compression = new LZ4Compression();

    public static void main(String args[])
    {
        File inputDir = new File(DATA_DIR + File.separator + "test1");
        long startTime, endTime;

        for(CompressionType compressionType: CompressionType.values())
        {
            startTime = System.currentTimeMillis();
            for (File file : inputDir.listFiles()) {
                compress(file, getCompressionOutputDir(compressionType), compressionType);
            }
            endTime = System.currentTimeMillis();
            LOGGER.info("Total time taken to compress file in {}: {}", compressionType.toString(), (endTime - startTime));
        }

    }

    public static File getCompressionOutputDir(CompressionType compressionType)
    {
        return new File(DATA_DIR+ File.separator + compressionType.toString() + File.separator);
    }


    public static void compress(File inputFileName, File outputDir, CompressionType compressionType)
    {
        outputDir.mkdirs();
        //LOGGER.info("{}: Input file name: {}", compression.getCompressionType(), inputFileName);
        File outputFile = new File(outputDir, inputFileName.getName());
        try (OutputStream outputStream = new FileOutputStream(outputFile)) {
                    Iterator<byte[]> iterator = new ChunkedStream(new FileInputStream(inputFileName), 2 * 1024 * 1024, compressionType);
                    while (iterator.hasNext()) {
                        byte[] chunk = iterator.next();
                        LOGGER.info("Chunk size: {}", chunk.length);
                        outputStream.write(chunk);
                    }

        } catch (Exception e) {
            //LOGGER.error("Error while trying to compress {} via {} and saving at outputDir: {}", inputFileName, compression.getCompressionType(), outputDir);
            LOGGER.error(e.getMessage());
        }
    }


}
