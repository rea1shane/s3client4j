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

    @Test
    public void testListBuckets() {
        List<Bucket> buckets = S3.listBuckets();
        System.out.println("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }

    @Test
    public void testCheckBucketExist() {
        System.out.println(S3.checkBucketExist(bucketName));
    }

    @Test
    public void testGetBucket() {
        System.out.println(S3.getBucket(bucketName));
    }

    @Test
    public void testCreateBucket() {
        System.out.println(S3.createBucket(bucketName));
    }

    @Test
    public void testCheckExistAndCreateBucket() {
        System.out.println(S3.checkExistAndCreateBucket(bucketName));
    }

    @Test
    public void testDeleteBucket() {
        System.out.println(S3.deleteBucket(bucketName));
    }

    @Test
    public void testPutObject() {
        System.out.println(S3.putObject(bucketName, desktopFilePath));
    }

    @Test
    public void testPutObjectWithKey() {
        System.out.println(S3.putObject(bucketName, keyName, desktopFilePath));
    }

    @Test
    public void testListObjects() {
        ObjectListing objectListing = S3.listObjects(bucketName);
        System.out.format("Objects in S3 bucket [%s] are:\n", bucketName);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    @Test
    public void testListObjectsWithPrefix() {
        ObjectListing objectListing = S3.listObjects(bucketName, prefix);
        System.out.format("Objects in S3 bucket [%s] with prefix [%s] are:\n", bucketName, prefix);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

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
