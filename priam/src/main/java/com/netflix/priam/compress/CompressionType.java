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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.priam.scheduler.UnsupportedTypeException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by aagrawal on 6/28/17.
 */
public enum CompressionType {
    FILE_LEVEL_SNAPPY("SNAPPY"), FILE_LEVEL_LZ4("LZ4"), NO_COMPRESSION("NO_COMPRESSION");

    private static final Logger logger = LoggerFactory.getLogger(CompressionType.class);
    private final String compressionType;
    private static final Map<String, CompressionType> lookupMap = new HashMap<>();

    static {
        for (CompressionType compressionType : CompressionType.values())
            lookupMap.put(compressionType.getCompressionType(), compressionType);
    }

    CompressionType(String compressionType) {
        this.compressionType = compressionType.toUpperCase();
    }

    /*
     * Helper method to find the compression type - case insensitive as user may put value which are not right case.
     * This returns the CompressionType if one is found. Refer to table below to understand the use-case.
     *
     * CompressionTypeValue|acceptNullorEmpty|acceptIllegalValue|Result
     * Valid value       |NA               |NA                |SchedulerType
     * Empty string      |True             |NA                |NULL
     * NULL              |True             |NA                |NULL
     * Empty string      |False            |NA                |UnsupportedTypeException
     * NULL              |False            |NA                |UnsupportedTypeException
     * Illegal value     |NA               |True              |NULL
     * Illegal value     |NA               |False             |UnsupportedTypeException
     */

    public static CompressionType lookup(String compressionType, boolean acceptNullOrEmpty, boolean acceptIllegalValue) throws UnsupportedTypeException {
        if (StringUtils.isEmpty(compressionType))
            if (acceptNullOrEmpty)
                return null;
            else {
                String message = String.format("%s is not a supported CompressionType. Supported values are %s", compressionType, getSupportedValues());
                logger.error(message);
                throw new UnsupportedTypeException(message);
            }


                if (lookupMap.containsKey(compressionType.trim().toUpperCase()))
            return lookupMap.get(compressionType.trim().toUpperCase());
                else
         {
            String message = String.format("%s is not a supported CompressionType. Supported values are %s", compressionType, getSupportedValues());

            if (acceptIllegalValue) {
                message = message + ". Since acceptIllegalValue is set to True, returning NULL instead.";
                logger.error(message);
                return null;
            }

            logger.error(message);
            throw new UnsupportedTypeException(message);
        }
    }

    private static String getSupportedValues() {
        StringBuffer supportedValues = new StringBuffer();
        boolean first = true;
        for (CompressionType type : CompressionType.values()) {
            if (!first)
                supportedValues.append(",");
            supportedValues.append(type);
            first = false;
        }

        return supportedValues.toString();
    }

    public static CompressionType lookup(String compressionType) throws UnsupportedTypeException {
        return lookup(compressionType, false, false);
    }

    public String getCompressionType() {
        return compressionType;
    }
}
