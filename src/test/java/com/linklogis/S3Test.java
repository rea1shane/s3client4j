package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.linklogis.override.ListObjectsRequest;
import org.junit.Test;

import java.util.List;

public class S3Test {

    String bucketName = "create-by-java-sdk";
    String desktopFilePath = "/Users/shane/Desktop/response2.json";
    String downloadsFilePath = "/Users/shane/Downloads/response2.json";
    String keyName = "response.json";
    String prefix = "test";

    /**
     * {@link S3#listBuckets()}
     */
    @Test
    public void testListBuckets() {
        List<Bucket> buckets = S3.listBuckets();
        System.out.println("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }

    /**
     * {@link S3#checkBucketExist(String)}
     */
    @Test
    public void testCheckBucketExist() {
        System.out.println(S3.checkBucketExist(bucketName));
    }

    /**
     * {@link S3#getBucket(String)}
     */
    @Test
    public void testGetBucket() {
        System.out.println(S3.getBucket(bucketName));
    }

    /**
     * {@link S3#createBucket(String)}
     */
    @Test
    public void testCreateBucket() {
        System.out.println(S3.createBucket(bucketName));
    }

    /**
     * {@link S3#checkExistAndCreateBucket(String)}
     */
    @Test
    public void testCheckExistAndCreateBucket() {
        System.out.println(S3.checkExistAndCreateBucket(bucketName));
    }

    /**
     * {@link S3#deleteBucket(String)}
     */
    @Test
    public void testDeleteBucket() {
        System.out.println(S3.deleteBucket(bucketName));
    }

    /**
     * {@link S3#putObject(String, String)}
     */
    @Test
    public void testPutObject() {
        System.out.println(S3.putObject(bucketName, desktopFilePath));
    }

    /**
     * {@link S3#putObject(String, String, String)}
     */
    @Test
    public void testPutObjectWithKey() {
        System.out.println(S3.putObject(bucketName, keyName, desktopFilePath));
    }

    /**
     * {@link S3#listObjects(String)}
     */
    @Test
    public void testListObjects() {
        ObjectListing objectListing = S3.listObjects(bucketName);
        System.out.format("Objects in S3 bucket [%s] are:\n", bucketName);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * {@link S3#listObjects(String, String)}
     */
    @Test
    public void testListObjectsWithPrefix() {
        ObjectListing objectListing = S3.listObjects(bucketName, prefix);
        System.out.format("Objects in S3 bucket [%s] with prefix [%s] are:\n", bucketName, prefix);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * {@link S3#listObjects(ListObjectsRequest)}
     */
    @Test
    public void testListObjectsWithRequest() {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.setBucketName(bucketName);
        listObjectsRequest.setPrefix(prefix);

        ObjectListing objectListing = S3.listObjects(listObjectsRequest);
        System.out.println("ListObjectsRequest params are:");
        System.out.println(listObjectsRequest);
        System.out.println("Objects in S3 bucket are:");
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

}
