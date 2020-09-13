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
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.emc.object.Range;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.S3ObjectMetadata;
import com.emc.object.s3.bean.AbstractDeleteResult;
import com.emc.object.s3.bean.AccessControlList;
import com.emc.object.s3.bean.CompleteMultipartUploadResult;
import com.emc.object.s3.bean.DeleteObjectsResult;
import com.emc.object.s3.bean.DeleteSuccess;
import com.emc.object.s3.bean.CopyPartResult;
import com.emc.object.s3.bean.GetObjectResult;
import com.emc.object.s3.bean.ListObjectsResult;
import com.emc.object.s3.bean.CanonicalUser;
import com.emc.object.s3.bean.Grant;
import com.emc.object.s3.bean.PutObjectResult;
import com.emc.object.s3.bean.S3Object;
import com.emc.object.s3.request.CompleteMultipartUploadRequest;
import com.emc.object.s3.request.CopyPartRequest;
import com.emc.object.s3.request.DeleteObjectsRequest;
import com.emc.object.s3.request.PutObjectRequest;
import com.emc.object.s3.request.SetObjectAclRequest;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
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
           com.amazonaws.services.s3.model.PutObjectRequest awsRequest = new com.amazonaws.services.s3.model.PutObjectRequest(
                                                                             request.getBucketName(),
                                                                             request.getKey(),
                                                                             new ByteArrayInputStream(request.getObject().toString().getBytes(StandardCharsets.UTF_8)),
                                                                             null);
           com.amazonaws.services.s3.model.PutObjectResult awsObjectResult = client.putObject(awsRequest);
           PutObjectResult putObjectResult = new PutObjectResult();
           return putObjectResult;
    }

    @Synchronized
    @Override
    public void putObject(String bucketName, String key, Range range, Object object) {
       client.putObject(new com.amazonaws.services.s3.model.PutObjectRequest(bucketName, key, 
                                                                             new ByteArrayInputStream(object.toString().getBytes(StandardCharsets.UTF_8)),
                                                                             null
                                                                            ));
    }

    @Synchronized
    @Override
    public void setObjectAcl(String bucketName, String key, AccessControlList acl) {
       com.amazonaws.services.s3.model.AccessControlList awsAcl = getAwsAcl(acl);
       client.setObjectAcl(bucketName, key, awsAcl);
    }

    @Synchronized
    @Override
    public void setObjectAcl(SetObjectAclRequest request) {
       com.amazonaws.services.s3.model.SetObjectAclRequest setObjectAclRequest = new com.amazonaws.services.s3.model.SetObjectAclRequest(
                                                                                 request.getBucketName(),
                                                                                 request.getKey(),
                                                                                 getAwsAcl(request.getAcl()));
       
       client.setObjectAcl(setObjectAclRequest);
    }

    @Override
    public AccessControlList getObjectAcl(String bucketName, String key) {
       com.amazonaws.services.s3.model.AccessControlList result = client.getObjectAcl(bucketName, key); 
       return getAcl(result);
    }

    @Override
    public CopyPartResult copyPart(CopyPartRequest request) {
       com.amazonaws.services.s3.model.CopyPartRequest awsRequest = new com.amazonaws.services.s3.model.CopyPartRequest()
                                                                        .withSourceBucketName(request.getSourceBucketName())
                                                                        .withSourceKey(request.getSourceKey())
                                                                        .withDestinationBucketName(request.getBucketName())
                                                                        .withDestinationKey(request.getKey())
                                                                        .withUploadId(request.getUploadId());
       com.amazonaws.services.s3.model.CopyPartResult awsResult = client.copyPart(awsRequest);
       CopyPartResult result = new CopyPartResult();
       result.setPartNumber(awsResult.getPartNumber());
       return result;
    }

    @Override
    public void deleteObject(String bucketName, String key) {
        client.deleteObject(bucketName, key);
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) {
       java.util.List<String> keys = request.getDeleteObjects().getKeys().stream().map(t -> t.getKey().toString()).collect(Collectors.toList());
       com.amazonaws.services.s3.model.DeleteObjectsRequest deleteObjectsRequest = new com.amazonaws.services.s3.model.DeleteObjectsRequest(request.getBucketName());
       deleteObjectsRequest.setKeys(keys.stream().map(key -> new com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion(key, null)).collect(Collectors.toList()));
       com.amazonaws.services.s3.model.DeleteObjectsResult deleteObjectsResult = client.deleteObjects(deleteObjectsRequest);
       java.util.List<AbstractDeleteResult> deleted = deleteObjectsResult.getDeletedObjects().stream().map(t -> { 
                                           DeleteSuccess object = new DeleteSuccess();
                                           object.setKey(t.getKey());
                                           return (AbstractDeleteResult) object; }).collect(Collectors.toList());
       DeleteObjectsResult result = new DeleteObjectsResult();
       result.setResults(deleted);
       return result;
    }

    @Override
    public ListObjectsResult listObjects(String bucketName, String prefix) {
        com.amazonaws.services.s3.model.ListObjectsRequest awsRequest  = new com.amazonaws.services.s3.model.ListObjectsRequest()
                                                                             .withBucketName(bucketName)
                                                                             .withPrefix(prefix);
        com.amazonaws.services.s3.model.ObjectListing awsResult = client.listObjects(awsRequest);
        ListObjectsResult result = new ListObjectsResult();
        java.util.List<S3Object> listed = awsResult.getObjectSummaries().stream().map(t -> {
                                          S3Object object = new S3Object();
                                          object.setKey(t.getKey());
                                          return object;
                                          }).collect(Collectors.toList());
        result.setObjects(listed);
        result.setBucketName(bucketName);
        result.setPrefix(prefix);
        return result;
    }

    @Override
    public S3ObjectMetadata getObjectMetadata(String bucketName, String key) {
        // TODO: return client.getObjectMetadata(bucketName, key);
        throw new RuntimeException("Unsupported");
    }

    @Override
    public InputStream readObjectStream(String bucketName, String key, Range range) {
        com.amazonaws.services.s3.model.S3Object object =  client.getObject(bucketName, key);
        return object.getObjectContent();
    }

    @Override
    public String initiateMultipartUpload(String bucketName, String key) {
        com.amazonaws.services.s3.model.InitiateMultipartUploadRequest awsRequest = new com.amazonaws.services.s3.model.InitiateMultipartUploadRequest(
                                                                                        bucketName, key);
        client.initiateMultipartUpload(awsRequest);
        return null;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request) {
        com.amazonaws.services.s3.model.CompleteMultipartUploadRequest awsRequest = new com.amazonaws.services.s3.model.CompleteMultipartUploadRequest(
                                                                                        request.getBucketName(), request.getKey(), null, null);
        com.amazonaws.services.s3.model.CompleteMultipartUploadResult awsResult  = client.completeMultipartUpload(awsRequest);
        CompleteMultipartUploadResult result = new CompleteMultipartUploadResult();
        result.setBucketName(request.getBucketName());
        result.setKey(request.getKey());
        result.setLocation(awsResult.getLocation());
        result.setETag(awsResult.getETag());
        return result;
    }

    @Override
    public GetObjectResult<InputStream> getObject(String bucketName, String key) {
        com.amazonaws.services.s3.model.S3Object object =  client.getObject(bucketName, key);
        GetObjectResult<InputStream> result = new GetObjectResult<InputStream>();
        result.setObject(object.getObjectContent());
        return result;
    }

    private com.amazonaws.services.s3.model.AccessControlList getAwsAcl(AccessControlList acl) {
       com.amazonaws.services.s3.model.AccessControlList awsAcl = new com.amazonaws.services.s3.model.AccessControlList();
       com.amazonaws.services.s3.model.Owner owner = new com.amazonaws.services.s3.model.Owner(acl.getOwner().getId(), acl.getOwner().getDisplayName());
       awsAcl.setOwner(owner);
       java.util.Set<com.amazonaws.services.s3.model.Grant> grants = new java.util.HashSet<com.amazonaws.services.s3.model.Grant>();
       for (Grant grant : acl.getGrants()) {
            com.amazonaws.services.s3.model.Grant g = new com.amazonaws.services.s3.model.Grant(new com.amazonaws.services.s3.model.CanonicalGrantee(((CanonicalUser) grant.getGrantee()).getId()), getAwsPermission(grant.getPermission()));
            grants.add(g);
       }
       return awsAcl;
    }

   private AccessControlList getAcl(com.amazonaws.services.s3.model.AccessControlList awsAcl) {
       AccessControlList acl = new AccessControlList();
       acl.setOwner(new CanonicalUser(awsAcl.getOwner().getId(), awsAcl.getOwner().getId()));
       for (com.amazonaws.services.s3.model.Grant grant : awsAcl.getGrants()) {
           Grant g = new Grant(new CanonicalUser(grant.getGrantee().getIdentifier(), ((CanonicalGrantee) grant.getGrantee()).getDisplayName()), this.getPermission(grant.getPermission()));
           acl.addGrants(g);
       }
       return acl;
   }

   private com.amazonaws.services.s3.model.Permission getAwsPermission(com.emc.object.s3.bean.Permission permission) {
        com.amazonaws.services.s3.model.Permission result = null;
        if (permission == com.emc.object.s3.bean.Permission.READ) { 
            result =  com.amazonaws.services.s3.model.Permission.Read; 
        }
        if (permission == com.emc.object.s3.bean.Permission.WRITE) { 
            result =  com.amazonaws.services.s3.model.Permission.Write; 
        }
        if (permission == com.emc.object.s3.bean.Permission.READ_ACP) { 
            result = com.amazonaws.services.s3.model.Permission.ReadAcp; 
        }
        if (permission == com.emc.object.s3.bean.Permission.WRITE_ACP) { 
            result = com.amazonaws.services.s3.model.Permission.WriteAcp; 
        }
        if (permission == com.emc.object.s3.bean.Permission.FULL_CONTROL) { 
            result = com.amazonaws.services.s3.model.Permission.FullControl; 
        } 
        return result;
   }

   private com.emc.object.s3.bean.Permission getPermission(com.amazonaws.services.s3.model.Permission permission) {
          com.emc.object.s3.bean.Permission result = null;
          if (permission == com.amazonaws.services.s3.model.Permission.Read) { 
              result = com.emc.object.s3.bean.Permission.READ; 
          }
          if (permission == com.amazonaws.services.s3.model.Permission.Write) { 
              result = com.emc.object.s3.bean.Permission.WRITE; 
          }
          if (permission == com.amazonaws.services.s3.model.Permission.ReadAcp) { 
              result = com.emc.object.s3.bean.Permission.READ_ACP; 
          }
          if (permission == com.amazonaws.services.s3.model.Permission.WriteAcp) { 
              result = com.emc.object.s3.bean.Permission.WRITE_ACP; 
          }
          if (permission == com.amazonaws.services.s3.model.Permission.FullControl) { 
              result = com.emc.object.s3.bean.Permission.FULL_CONTROL; 
          }
          return result;
   }
}
