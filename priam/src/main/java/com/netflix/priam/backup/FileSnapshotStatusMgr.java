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

package com.netflix.priam.backup;

import com.google.inject.Inject;
import com.netflix.priam.IConfiguration;
import com.netflix.priam.utils.MaxSizeHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.*;
import java.util.*;

/**
 * Created by aagrawal on 7/11/17.
 */
@Singleton
public class FileSnapshotStatusMgr extends BackupStatusMgr {
    private static final Logger logger = LoggerFactory.getLogger(FileSnapshotStatusMgr.class);
    private static final int IN_MEMORY_SNAPSHOT_CAPACITY = 60;
    private IConfiguration config;
    private String filename;

    @Inject
    public FileSnapshotStatusMgr(IConfiguration config) {
        super(IN_MEMORY_SNAPSHOT_CAPACITY); //Fetch capacity from properties, if required.
        this.config = config;
        this.filename = this.config.getBackupStatusFileLoc();
        init();
    }

    private void init()
    {
        //Retrieve entire file and re-populate the list.
        File snapshotFile = new File(filename);
        if (!snapshotFile.exists())
        {
            logger.info("Snapshot status file do not exist on system. Bypassing initilization phase.");
            return;
        }

        try(final ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(snapshotFile));)
        {
            backupMetadataMap = (Map<String, LinkedList<BackupMetadata>>)inputStream.readObject();
            logger.info("Snapshot status of size {} fetched successfully from {}", backupMetadataMap.size(), filename);
        } catch (IOException e) {
            logger.error("Error while trying to fetch snapshot status from {}. Error: {}. If this is first time after upgrading Priam, ignore this.", filename, e.getLocalizedMessage());
            e.printStackTrace();
        } catch (Exception e) {
            logger.error("Error while trying to fetch snapshot status from {}. Error: {}.", filename, e.getLocalizedMessage());
            e.printStackTrace();
        }

        if (backupMetadataMap == null)
            backupMetadataMap = new MaxSizeHashMap<>(capacity);
    }

    @Override
    void save(BackupMetadata backupMetadata) {
        //Will save entire list to file.
        try(final ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));) {
            out.writeObject(backupMetadataMap);
            out.flush();
            logger.info("Snapshot status of size {} is saved to {}", backupMetadataMap.size(), filename);
        }catch (IOException e) {
            logger.error("Error while trying to persist snapshot status to {}. Error: {}", filename, e.getLocalizedMessage());
        }
    }

    @Override
    LinkedList<BackupMetadata> fetch(String snapshotDate) {
        //No need to fetch from local machine as it was read once at start. No point reading again and again.
        return backupMetadataMap.get(snapshotDate);
    }
}
