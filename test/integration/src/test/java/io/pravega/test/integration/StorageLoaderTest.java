/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.storage.extendeds3.ExtendedS3SimpleStorageFactory;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.extendeds3.ExtendedS3StorageFactory;
import io.pravega.storage.hdfs.HDFSSimpleStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageConfig;
import io.pravega.storage.hdfs.HDFSStorageFactory;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageLoaderTest {

    private StorageFactory expectedFactory;

    @Test
    public void testHDFSStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, HDFSStorageConfig.builder().build());
        val factory  = getStorageFactory(configSetup, storageType, "HDFS", StorageLayoutType.ROLLING_STORAGE);
        assertTrue(factory instanceof HDFSStorageFactory);
    }

    @Test
    public void testHDFSSimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, HDFSStorageConfig.builder().build());
        val factory = getStorageFactory(configSetup, storageType, "HDFS", StorageLayoutType.CHUNKED_STORAGE);
        assertTrue(factory instanceof HDFSSimpleStorageFactory);
    }

    @Test
    public void testExtendedS3Storage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        ConfigSetup configSetup = mock(ConfigSetup.class);

        val config = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                .with(ExtendedS3StorageConfig.BUCKET, "bucket")
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, config);

        val factory = getStorageFactory(configSetup, storageType, "EXTENDEDS3", StorageLayoutType.ROLLING_STORAGE);
        assertTrue(factory instanceof ExtendedS3StorageFactory);
    }

    @Test
    public void testExtendedS3SimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val config = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                .with(ExtendedS3StorageConfig.BUCKET, "bucket")
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        StorageExtraConfig extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, config);

        val factory = getStorageFactory(configSetup, storageType, "EXTENDEDS3", StorageLayoutType.CHUNKED_STORAGE);
        assertTrue(factory instanceof ExtendedS3SimpleStorageFactory);
    }

    private StorageFactory getStorageFactory(ConfigSetup setup, ServiceConfig.StorageType storageType, String name, StorageLayoutType storageLayoutType) throws DurableDataLogException {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        StorageLoader loader = new StorageLoader();
        return loader.load(setup, name, storageLayoutType, executor);
    }
}
