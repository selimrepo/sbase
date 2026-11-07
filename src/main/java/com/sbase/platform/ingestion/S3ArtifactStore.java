package com.sbase.platform.ingestion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.HexFormat;

@Service
public class S3ArtifactStore implements AutoCloseable {
 private final S3Client client; private final String bucket;
 public S3ArtifactStore(@Value("${sbase.s3.endpoint:}") String endpoint,@Value("${sbase.s3.region:us-east-1}") String region,
  @Value("${sbase.s3.access-key:}") String access,@Value("${sbase.s3.secret-key:}") String secret,@Value("${sbase.s3.bucket:sbase-artifacts}") String bucket){
  this.bucket=bucket;var b=S3Client.builder().region(Region.of(region)).serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build());
  if(!endpoint.isBlank())b.endpointOverride(java.net.URI.create(endpoint));
  if(!access.isBlank()&&!secret.isBlank())b.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(access,secret)));
  else b.credentialsProvider(DefaultCredentialsProvider.create());
  client=b.build();
 }
 public String put(String key,InputStream input,long size,String contentType){
  client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),software.amazon.awssdk.core.sync.RequestBody.fromInputStream(input,size));
  return "s3://"+bucket+"/"+key;
 }
 public ResponseInputStream<GetObjectResponse> get(String key){return client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());}
 public void delete(String key){client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());}
 public void close(){client.close();}
}
