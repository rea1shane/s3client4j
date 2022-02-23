package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.linklogis.override.ListObjectsRequest;
import org.junit.Test;

import java.util.List;

public class S3Test {

    final static S3 s3Object = new S3();

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
        List<Bucket> buckets = s3Object.listBuckets();
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
        System.out.println(s3Object.checkBucketExist(bucketName));
    }

    /**
     * {@link S3#getBucket(String)}
     */
    @Test
    public void testGetBucket() {
        System.out.println(s3Object.getBucket(bucketName));
    }

    /**
     * {@link S3#createBucket(String)}
     */
    @Test
    public void testCreateBucket() {
        System.out.println(s3Object.createBucket(bucketName));
    }

    /**
     * {@link S3#checkExistAndCreateBucket(String)}
     */
    @Test
    public void testCheckExistAndCreateBucket() {
        System.out.println(s3Object.checkExistAndCreateBucket(bucketName));
    }

    /**
     * {@link S3#deleteBucket(String)}
     */
    @Test
    public void testDeleteBucket() {
        System.out.println(s3Object.deleteBucket(bucketName));
    }

    /**
     * {@link S3#putObject(String, String)}
     */
    @Test
    public void testPutObject() {
        System.out.println(s3Object.putObject(bucketName, desktopFilePath));
    }

    /**
     * {@link S3#putObject(String, String, String)}
     */
    @Test
    public void testPutObjectWithKey() {
        System.out.println(s3Object.putObject(bucketName, keyName, desktopFilePath));
    }

    /**
     * {@link S3#listObjects(String)}
     */
    @Test
    public void testListObjects() {
        ObjectListing objectListing = s3Object.listObjects(bucketName);
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
        ObjectListing objectListing = s3Object.listObjects(bucketName, prefix);
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

        ObjectListing objectListing = s3Object.listObjects(listObjectsRequest);
        System.out.println("ListObjectsRequest params are:");
        System.out.println(listObjectsRequest);
        System.out.println("Objects in S3 bucket are:");
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

}
