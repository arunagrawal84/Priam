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





//****************** 2.X only
//
//import org.apache.cassandra.config.CFMetaData;
//import org.apache.cassandra.db.ColumnFamilyType;
//import org.apache.cassandra.dht.IPartitioner;
//import org.apache.cassandra.exceptions.ConfigurationException;
//import org.apache.cassandra.io.FSWriteError;
//import org.apache.cassandra.io.sstable.Descriptor;
//import org.apache.cassandra.io.sstable.*;
//import org.apache.cassandra.io.sstable.metadata.*;
//import org.apache.cassandra.utils.FBUtilities;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.EnumSet;
//import java.util.Map;
//
///**
// * Created by aagrawal on 6/12/17.
// */
//
//public class SSTableCompressor {
//    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SSTableCompressor.class);
//    private String dataDbFileName;
//    private Descriptor inputSSTableDescriptor;
//    private final SSTableLoader.Client client;
//
//    public SSTableCompressor(SSTableLoader.Client client) {
//        this.client = client;
//    }
//
//
//    public void setDataDbFileName(String dataDbFileName) throws Exception
//    {
//        if (dataDbFileName == null || dataDbFileName.isEmpty())
//            throw new Exception("Null or emtpy file name is not allowed to coompress");
//
//        //Ensure file exists.
//        File inputSSTableFullPath = new File(dataDbFileName);
//        if (!inputSSTableFullPath.exists() || !inputSSTableFullPath.canRead())
//            throw new Exception("File do not exist or no read permission.");
//
//        this.dataDbFileName = dataDbFileName;
//
//        inputSSTableDescriptor = Descriptor.fromFilename(inputSSTableFullPath.getAbsolutePath());
//    }
//
//    public void compress() throws Exception
//    {
//        SSTableWriter writer = null;
//
//        try {
//
//
////            /*outputCFMetaData.compressionParameters()//createNewCFMetaData(inputSSTableDescriptor, inputCFMetaData);
////
////            SSTableReader inputSStable = SSTableReader.openNoValidation(inputSSTableDescriptor, inputCFMetaData);
////            writer = createSSTableWriter(inputSSTableDescriptor, outputCFMetaData, inputSStable);
////
////            final ISSTableScanner currentScanner = inputSStable.getScanner();
////
////            while (currentScanner.hasNext()) {
////                UnfilteredRowIterator row = currentScanner.next();
////                writer.append(row);
////            }
////            writer.finish(false);*/
////        } catch (IOException e) {
////            e.printStackTrace(System.err);
////        }
//// finally {
////            FileUtils.closeQuietly(writer);
//        }catch (Exception e)
//        {
//            throw new Exception("error while compressing" + e.toString());
//        }
//
//    }
//
//
//    /**
//     * Construct table schema from info stored in SSTable's Stats.db
//     *
//     * @param desc SSTable's descriptor
//     * @return Restored CFMetaData
//     * @throws IOException when Stats.db cannot be read
//     */
//    public CFMetaData metadataFromSSTable(Descriptor desc) throws IOException, ConfigurationException {
//        if (!desc.version.equals("ka"))
//            throw new IOException("pre-2.1 SSTable is not supported.");
//
//        CFMetaData metadata = client.getCFMetaData(desc.ksname, desc.cfname);
////        CFMetaData inputCFMetaData = new CFMetaData(desc.ksname, desc.cfname,
////                ColumnFamilyType.Standard, null);//TODO);
//
//        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.VALIDATION);
//        Map<MetadataType, MetadataComponent> sstableMetadata = desc.getMetadataSerializer().deserialize(desc, types);
//        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
//        IPartitioner partitioner = FBUtilities.newPartitioner(validationMetadata.partitioner);
//        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
////
////        SerializationHeader.Component header = (SerializationHeader.Component) sstableMetadata.get(MetadataType.HEADER);
////        CFMetaData builder = CFMetaData.Builder.create(desc.ksname, desc.cfname).withPartitioner(partitioner);
////        header.getStaticColumns().entrySet().stream()
////                .forEach(entry -> {
////                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
////                    builder.addStaticColumn(ident, entry.getValue());
////                });
////        header.getRegularColumns().entrySet().stream()
////                .forEach(entry -> {
////                    ColumnIdentifier ident = ColumnIdentifier.getInterned(UTF8Type.instance.getString(entry.getKey()), true);
////                    builder.addRegularColumn(ident, entry.getValue());
////                });
////        builder.addPartitionKey("PartitionKey", header.getKeyType());
////        for (int i = 0; i < header.getClusteringTypes().size(); i++) {
////            builder.addClusteringColumn("clustering" + (i > 0 ? i : ""), header.getClusteringTypes().get(i));
////        }
////        return builder.build();
//    }
//
//
//
//    private static SSTableWriter createSSTableWriter(Descriptor inputSSTableDescriptor,
//                                                       CFMetaData outputCfmMetaData,
//                                                       SSTableReader inputSstable,
//                                                     String compressedDirectory) throws IOException, ConfigurationException {
//        logger.info("Output directory: " + compressedDirectory);
//
//        File outputDirectory = new File(compressedDirectory + File.separatorChar + inputSSTableDescriptor.ksname +
//                File.separatorChar + inputSSTableDescriptor.cfname);
//
//        if (!outputDirectory.exists() && !outputDirectory.mkdirs())
//            throw new FSWriteError(new IOException("failed to create tmp directory"), outputDirectory.getAbsolutePath());
//
//        EnumSet<MetadataType> types = EnumSet.of(MetadataType.STATS, MetadataType.VALIDATION);
//        Map<MetadataType, MetadataComponent> sstableMetadata = inputSSTableDescriptor.getMetadataSerializer().deserialize(inputSSTableDescriptor, types);
//        ValidationMetadata validationMetadata = (ValidationMetadata) sstableMetadata.get(MetadataType.VALIDATION);
//        IPartitioner partitioner = FBUtilities.newPartitioner(validationMetadata.partitioner);
//        StatsMetadata statsMetadata = (StatsMetadata) sstableMetadata.get(MetadataType.STATS);
//
//        Schema.instance.getCFMetaData(inputSSTableDescriptor);
//
//        String filename = "";
//        long keyCount = 0L;
//        return new SSTableWriter( filename,
//         keyCount,
//         statsMetadata.repairedAt, outputCfmMetaData,
//        partitioner,
//        new MetadataCollector(outputCfmMetaData.comparator).sstableLevel(inputSstable.getSSTableMetadata().sstableLevel));
//    }
//
//}
