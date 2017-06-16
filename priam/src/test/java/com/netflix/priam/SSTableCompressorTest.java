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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.netflix.priam.aws.auth.SSTableCompressor3;
import com.netflix.priam.backup.BRTestModule;
import com.netflix.priam.backup.FakeBackupFileSystem;
import com.netflix.priam.backup.IBackupFileSystem;
import com.netflix.priam.backup.TestBackup;
import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Created by aagrawal on 6/13/17.
 */
public class SSTableCompressorTest {
    private static Injector injector;
    private static SSTableCompressor3 ssTableCompressor;
    private static final Logger logger = LoggerFactory.getLogger(SSTableCompressorTest.class);

    @BeforeClass
    public static void setup() throws InterruptedException, IOException
    {
        //injector = Guice.createInjector(new BRTestModule());
    }

    @Test
    public void testCompression() throws Exception
    {
//        String inputDir = "src/test/resources/pappyperftest/test1-6924dae01bd011e7a6fa2d550a288353";
//        String filename = "/mc-69-big-Data.db";
        String inputDir = "src/test/resources/data";
        String filename = "/tb2/mc-1-big-Data.db";
        File outputLocation = new File(inputDir, "compressed");
        ssTableCompressor = new SSTableCompressor3();
        ssTableCompressor.compress(inputDir + filename, outputLocation.getAbsolutePath());
        Collection<File> compressedFiles = FileUtils.listFiles(outputLocation, null, true);
        for(File file:compressedFiles)
        {
            logger.info(file.getAbsolutePath());
        }
    }

}
