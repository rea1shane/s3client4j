package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;
import org.junit.Test;

import java.util.List;

public class S3Test {

    String bucketName = "create-by-java-sdk";

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
}
