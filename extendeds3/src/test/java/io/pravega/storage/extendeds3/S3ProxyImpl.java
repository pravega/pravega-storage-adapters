/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import java.io.InputStream;
import lombok.Synchronized;
import io.findify.s3mock.S3Mock;

/**
 * Extended S3 server emulation implementation based on s3Mock.
 * S3Proxy service runs in proc and the client talks to it using REST.
 */
public class S3ProxyImpl extends S3ImplBase {
    private final S3Mock api;
    private final AmazonS3 client;

    public S3ProxyImpl(String endpointLink, S3Config s3Config) {
    api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
            
    EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
    client = AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(true)  
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))     
      .build();
    }

    @Override
    public void start() throws Exception {
        api.start();
    }

    @Override
    public void stop() throws Exception {
        api.shutdown();
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest request) {
           throw new RuntimeException("Unsupported");
    }

    @Synchronized
    @Override
    public void putObject(String bucketName, String key, Range range, Object object) {
       client.putObject(new com.amazonaws.services.s3.model.PutObjectRequest(bucketName, key, object.toString()));
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
       // TODO: fill acl
       // client.setObjectAcl(bucketName, key, null);
       throw new RuntimeException("Unsupported");
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
         throw new RuntimeException("Unsupported");
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
       com.amazonaws.services.s3.model.AccessControlList result = client.getObjectAcl(bucketName, key); 
       // TODO: fill acl
       // return new AccessControlList();
       throw new RuntimeException("Unsupported");
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
        // TODO: return client.copyPart(request);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        client.deleteObject(bucketName, key);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
        // TODO: return client.deleteObjects(request);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        // TODO: return client.listObjects(bucketName, prefix);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        // TODO: return client.getObjectMetadata(bucketName, key);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public InputStream readObjectStream(String bucketName, String key, Range range) {
        S3Object object =  client.getObject(bucketName, key);
        return object.getObjectContent();
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        // TODO: return client.initiateMultipartUpload(bucketName, key);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        //  TODO: return client.completeMultipartUpload(request);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        S3Object object =  client.getObject(bucketName, key);
        GetObjectResult<InputStream> result = new GetObjectResult<InputStream>();
        result.setObject(object.getObjectContent());
        return result;
    }
}
