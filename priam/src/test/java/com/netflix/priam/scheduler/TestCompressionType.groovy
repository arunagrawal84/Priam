package com.netflix.priam.scheduler

import com.netflix.priam.compress.CompressionType

/**
 * Created by aagrawal on 3/16/17.
 * This is used to test CompressionType with all the values you might get.
 */
import spock.lang.*

@Unroll
class TestCompressionType extends Specification{

    def "Exception for value #compressionType , #acceptNullorEmpty , #acceptIllegalValue"() {
        when:
        CompressionType.lookup(compressionType, acceptNullorEmpty, acceptIllegalValue)

        then:
        def error = thrown(expectedException)

        where:
        compressionType  | acceptNullorEmpty | acceptIllegalValue || expectedException
        "sdf"          | true              | false              || UnsupportedTypeException
        ""             | false             | true               || UnsupportedTypeException
        null           | false             | true               || UnsupportedTypeException

    }

    def "SchedulerType for value #compressionType , #acceptNullorEmpty , #acceptIllegalValue is #result"() {
        expect:
        CompressionType.lookup(compressionType, acceptNullorEmpty, acceptIllegalValue) == result

        where:
        compressionType | acceptNullorEmpty | acceptIllegalValue || result
        "SNAPPY"        | true              | true               || CompressionType.FILE_LEVEL_SNAPPY
        "Snappy"        | true              | true               || CompressionType.FILE_LEVEL_SNAPPY
        "LZ4"        | true              | true               || CompressionType.FILE_LEVEL_LZ4
        "lz4"        | true              | true              || CompressionType.FILE_LEVEL_LZ4
        "NO_COMPRESSION"        | true              | true              || CompressionType.NO_COMPRESSION
        "NO_compression"        | true              | true              || CompressionType.NO_COMPRESSION

        "SNAPPY"        | true              | false               || CompressionType.FILE_LEVEL_SNAPPY
        "Snappy"        | true              | false               || CompressionType.FILE_LEVEL_SNAPPY
        "LZ4"        | true              | false               || CompressionType.FILE_LEVEL_LZ4
        "lz4"        | true              | false              || CompressionType.FILE_LEVEL_LZ4
        "NO_COMPRESSION"        | true              | false              || CompressionType.NO_COMPRESSION
        "NO_compression"        | true              | false              || CompressionType.NO_COMPRESSION

        "SNAPPY"        | false              | false               || CompressionType.FILE_LEVEL_SNAPPY
        "Snappy"        | false              | false               || CompressionType.FILE_LEVEL_SNAPPY
        "LZ4"        | false              | false               || CompressionType.FILE_LEVEL_LZ4
        "lz4"        | false              | false              || CompressionType.FILE_LEVEL_LZ4
        "NO_COMPRESSION"        | false              | false              || CompressionType.NO_COMPRESSION
        "NO_compression"        | false              | false              || CompressionType.NO_COMPRESSION

        "SNAPPY"        | false              | true               || CompressionType.FILE_LEVEL_SNAPPY
        "Snappy"        | false              | true               || CompressionType.FILE_LEVEL_SNAPPY
        "LZ4"        | false              | true               || CompressionType.FILE_LEVEL_LZ4
        "lz4"        | false              | true              || CompressionType.FILE_LEVEL_LZ4
        "NO_COMPRESSION"        | false              | true              || CompressionType.NO_COMPRESSION
        "NO_compression"        | false              | true              || CompressionType.NO_COMPRESSION

        ""            | true              | false              || null
        null          | true              | false              || null
        "sdf"         | false             | true               || null
        "sdf"         | true              | true               || null
    }


}
