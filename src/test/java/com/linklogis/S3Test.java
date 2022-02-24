package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.linklogis.override.ListObjectsRequest;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class S3Test {

    final static S3 s3Instance = new S3();

    String bucketName = "create-by-java-sdk";
    String desktopFilePath = "/Users/shane/Desktop/response2.json";
    String downloadsFilePath = "/Users/shane/Downloads/response2.json";
    String key = "response.json";
    String prefix = "test";

    /**
     * <p>
     * {@link S3#listBuckets()}
     * </p>
     */
    @Test
    public void testListBuckets() {
        List<Bucket> buckets = s3Instance.listBuckets();
        System.out.println("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }

    /**
     * <p>
     * {@link S3#checkBucketExist(String)}
     * </p>
     */
    @Test
    public void testCheckBucketExist() {
        System.out.println(s3Instance.checkBucketExist(bucketName));
    }

    /**
     * <p>
     * {@link S3#getBucket(String)}
     * </p>
     */
    @Test
    public void testGetBucket() {
        System.out.println(s3Instance.getBucket(bucketName));
    }

    /**
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     */
    @Test
    public void testCreateBucket() {
        System.out.println(s3Instance.createBucket(bucketName));
    }

    /**
     * <p>
     * {@link S3#deleteBucket(String)}
     * </p>
     */
    @Test
    public void testDeleteBucket() {
        System.out.println(s3Instance.deleteBucket(bucketName));
    }

    /**
     * <p>
     * {@link S3#listObjects(String)}
     * </p>
     */
    @Test
    public void testListObjects() {
        ObjectListing objectListing = s3Instance.listObjects(bucketName);
        System.out.format("Objects in S3 bucket [%s] are:\n", bucketName);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * <p>
     * {@link S3#listObjects(String, String)}
     * </p>
     */
    @Test
    public void testListObjectsWithPrefix() {
        ObjectListing objectListing = s3Instance.listObjects(bucketName, prefix);
        System.out.format("Objects in S3 bucket [%s] with prefix [%s] are:\n", bucketName, prefix);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * <p>
     * {@link S3#listObjects(ListObjectsRequest)}
     * </p>
     */
    @Test
    public void testListObjectsWithRequest() {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        listObjectsRequest.setPrefix(prefix);

        ObjectListing objectListing = s3Instance.listObjects(listObjectsRequest);
        System.out.println("ListObjectsRequest params are:");
        System.out.println(listObjectsRequest);
        System.out.println("Objects in S3 bucket are:");
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * <p>
     * {@link S3#putObject(String, String, InputStream, ObjectMetadata)}
     * </p>
     */
    @Test
    public void testPutObjectWithStream() throws IOException {
        InputStream input = new FileInputStream(desktopFilePath);
        ObjectMetadata objectMetadata = new ObjectMetadata();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("usage", "test");
        objectMetadata.setUserMetadata(metadata);
        System.out.println(s3Instance.putObject(bucketName, key, input, objectMetadata));
        input.close();
    }

    /**
     * <p>
     * {@link S3#getObject(String, String)}
     * </p>
     * <p>
     * {@link S3#downloadObject(S3Object, OutputStream)}
     * </p>
     */
    @Test
    public void testDownloadObject() throws FileNotFoundException {
        FileOutputStream outputStream = new FileOutputStream(key);
        S3Object s3Object = s3Instance.getObject(bucketName, key);
        s3Instance.downloadObject(s3Object, outputStream);
    }

}
