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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.io.sstable.format.SSTableFlushObserver;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.format.SSTableWriter;
import org.apache.cassandra.io.sstable.format.big.BigTableWriter;
import org.apache.cassandra.io.sstable.metadata.MetadataCollector;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by aagrawal on 6/13/17.
 */
public class SSTableCompressor3 {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SSTableCompressor3.class);

    static {
        DatabaseDescriptor.clientInitialization(false);
        //Config.setClientMode(true);
    }

    public void compress(String dataDbFileName, String outputLocation) throws Exception
    {
        if (dataDbFileName == null || dataDbFileName.isEmpty())
            throw new Exception("Null or emtpy file name is not allowed to coompress");

        //Ensure file exists.
        File inputSSTableFullPath = new File(dataDbFileName);
        if (!inputSSTableFullPath.exists() || !inputSSTableFullPath.canRead())
            throw new Exception("File do not exist or no read permission." + inputSSTableFullPath.getAbsolutePath());

        File outputDir = new File(outputLocation);

        if (!outputDir.exists() && !outputDir.mkdirs())
            throw new FSWriteError(new IOException("failed to create tmp directory"), outputDir.getAbsolutePath());

        if (!outputDir.isDirectory())
            throw new Exception("output location is not directory: " + outputDir.getAbsolutePath());

        final Descriptor inputSSTableDescriptor= Descriptor.fromFilename(new File(dataDbFileName).getAbsolutePath());

        SSTableWriter writer = null;

        try {
            final CFMetaData inputCFMetaData = metadataFromSSTable(inputSSTableDescriptor);
            final CFMetaData outputCFMetaData = createNewCFMetaData(inputSSTableDescriptor, inputCFMetaData);

            final SSTableReader inputSStable = SSTableReader.openNoValidation(inputSSTableDescriptor, inputCFMetaData);
            writer = createSSTableWriter(inputSSTableDescriptor, outputCFMetaData, inputSStable, outputLocation);

            final ISSTableScanner currentScanner = inputSStable.getScanner();

            while (currentScanner.hasNext()) {
                UnfilteredRowIterator row = currentScanner.next();
                writer.append(row);
            }
            writer.finish(false);
        } catch (IOException e) {
            e.printStackTrace(System.err);
        } finally {
            FileUtils.closeQuietly(writer);
        }
    }


    private static class FlushObserver implements SSTableFlushObserver {
        private final Multimap<Pair<ByteBuffer, Long>, Cell> rows = ArrayListMultimap.create();
        private Pair<ByteBuffer, Long> currentKey;

        @Override
        public void begin() {
        }

        @Override
        public void startPartition(DecoratedKey key, long indexPosition) {
            currentKey = Pair.create(key.getKey(), indexPosition);
            //logger.info("Current key: " + new String(key.getKey().array()));
        }

        @Override
        public void nextUnfilteredCluster(Unfiltered row) {
            if (row.isRow())
                ((Row) row).forEach((c) -> {
                    rows.put(currentKey, (Cell) c);
                });
        }

        @Override
        public void complete() {
            logger.info("Complete writing with the last key: " + new String(currentKey.left.array()));
        }
    }


    /**
     * Construct table schema from info stored in SSTable's Stats.db
     *
     * @param desc SSTable's descriptor
     * @return Restored CFMetaData
     * @throws IOException when Stats.db cannot be read
     */
    public CFMetaData metadataFromSSTable(Descriptor desc) throws IOException {
        if (!desc.version.storeRows())
            throw new IOException("pre-3.0 SSTable is not supported.");

        final EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.HEADER);
        final Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
        final SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
        final IPartitioner partitioner = FBUtilities.newPartitioner(desc);

        final CFMetaData.Builder builder = CFMetaData.Builder.create(desc.ksname, desc.cfname).withPartitioner(partitioner);
        header.getStaticColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addStaticColumn(ident, entry.getValue());
                });
        header.getRegularColumns().entrySet().stream()
                .forEach(entry -> {
                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
                    builder.addRegularColumn(ident, entry.getValue());
                });
//        builder.addPartitionKey("PartitionKey", header.getKeyType());
//        for (int i = 0; i < header.getClusteringTypes().size(); i++) {
//            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
//        }
        List<String> partitionKeyNames = Collections.<String>emptyList();
        List<String> clusteringKeyNames = Collections.<String>emptyList();
        if (header.getKeyType() instanceof CompositeType) {
            assert partitionKeyNames.size() == 0
                    || partitionKeyNames.size() == ((CompositeType) header.getKeyType()).types.size();
            int counter = 0;
            for (AbstractType type: ((CompositeType) header.getKeyType()).types) {
                String partitionColName = "PartitionKey" + counter;
                if (partitionKeyNames.size() > 0) {
                    partitionColName = partitionKeyNames.get(counter);
                }
                builder.addPartitionKey(partitionColName, type);
                counter++;
            }
        } else {
            String partitionColName = "PartitionKey";
            if (partitionKeyNames.size() > 0) {
                partitionColName = partitionKeyNames.get(0);
            }
            builder.addPartitionKey(partitionColName, header.getKeyType());
        }

        for (int i = 0; i < header.getClusteringTypes().size(); i++) {
            assert clusteringKeyNames.size() == 0
                    || clusteringKeyNames.size() == header.getClusteringTypes().size();
            String clusteringColName = "clustering" + (i > 0 ? i : "");
            if (clusteringKeyNames.size() > 0) {
                clusteringColName = clusteringKeyNames.get(i);
            }
            builder.addClusteringColumn(clusteringColName, header.getClusteringTypes().get(i));
        }
        return builder.build();
    }

    private CFMetaData createNewCFMetaData(Descriptor inputSSTableDescriptor, CFMetaData metadata) {
        CFMetaData.Builder cfMetadataBuilder = CFMetaData.Builder.create(inputSSTableDescriptor.ksname, inputSSTableDescriptor.cfname);
        Collection<ColumnDefinition> colDefs = metadata.allColumns();

        for (ColumnDefinition colDef : colDefs) {
            switch (colDef.kind) {
                case PARTITION_KEY:
                    cfMetadataBuilder.addPartitionKey(colDef.name, colDef.cellValueType());
                    break;
                case CLUSTERING:
                    cfMetadataBuilder.addClusteringColumn(colDef.name, colDef.cellValueType());
                    break;
                case STATIC:
                    cfMetadataBuilder.addStaticColumn(colDef.name, colDef.cellValueType());
                    break;
                default:
                    cfMetadataBuilder.addRegularColumn(colDef.name, colDef.cellValueType());
            }
        }

        cfMetadataBuilder.withPartitioner(Murmur3Partitioner.instance);
        CFMetaData cfm = cfMetadataBuilder.build();
        cfm.compression(CompressionParams.DEFAULT);

        return cfm;
    }


    private SSTableWriter createSSTableWriter(Descriptor inputSSTableDescriptor,
                                                     CFMetaData outputCfmMetaData,
                                                     SSTableReader inputSstable, String outputLocation) {
        final File outputDirectory = new File(outputLocation + File.separatorChar + inputSSTableDescriptor.ksname +
                File.separatorChar + inputSSTableDescriptor.cfname);

        if (!outputDirectory.exists() && !outputDirectory.mkdirs())
            throw new FSWriteError(new IOException("failed to create tmp directory"), outputDirectory.getAbsolutePath());

        SSTableFormat.Type sstableFormat = SSTableFormat.Type.BIG;
        final Descriptor outDescriptor = new Descriptor(sstableFormat.info.getLatestVersion().getVersion(),
                outputDirectory,
                inputSSTableDescriptor.ksname, inputSSTableDescriptor.cfname,
                inputSSTableDescriptor.generation,
                sstableFormat);

        final BigTableWriter writer = new BigTableWriter(outDescriptor,
                inputSstable.getTotalRows(), 0L, outputCfmMetaData,
                new MetadataCollector(outputCfmMetaData.comparator).sstableLevel(inputSstable.getSSTableMetadata().sstableLevel),
                new SerializationHeader(true, outputCfmMetaData, outputCfmMetaData.partitionColumns(), EncodingStats.NO_STATS),
                Collections.singletonList(new FlushObserver()),
                LifecycleTransaction.offline(OperationType.UNKNOWN));


        return writer;
    }


}
