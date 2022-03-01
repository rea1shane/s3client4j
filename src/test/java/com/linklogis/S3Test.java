package com.linklogis;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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

    String sourceBucketName = "create-by-java-sdk";
    String destinationBucketName = "create-by-java-sdk2";
    String desktopFilePath = "/Users/shane/Desktop/response2.json";
    String downloadsFilePath = "/Users/shane/Downloads/response2.json";
    String sourceKey = "response.json";
    String destinationKey = "copy_test.json";
    String prefix = "test";
    String[] keys = {"delete1.json", "delete2.json", "delete3.json"};

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
        System.out.println(s3Instance.checkBucketExist(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#getBucket(String)}
     * </p>
     */
    @Test
    public void testGetBucket() {
        System.out.println(s3Instance.getBucket(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#createBucket(String)}
     * </p>
     */
    @Test
    public void testCreateBucket() {
        System.out.println(s3Instance.createBucket(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#deleteBucket(String)}
     * </p>
     */
    @Test
    public void testDeleteBucket() {
        System.out.println(s3Instance.deleteBucket(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#listObjects(String)}
     * </p>
     */
    @Test
    public void testListObjects() {
        ObjectListing objectListing = s3Instance.listObjects(sourceBucketName);
        System.out.format("Objects in S3 bucket [%s] are:\n", sourceBucketName);
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
        ObjectListing objectListing = s3Instance.listObjects(sourceBucketName, prefix);
        System.out.format("Objects in S3 bucket [%s] with prefix [%s] are:\n", sourceBucketName, prefix);
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
        System.out.println(s3Instance.putObject(sourceBucketName, sourceKey, input, objectMetadata));
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
        FileOutputStream outputStream = new FileOutputStream(sourceKey);
        S3Object s3Object = s3Instance.getObject(sourceBucketName, sourceKey);
        System.out.println(s3Instance.downloadObject(s3Object, outputStream));
    }

    /**
     * <p>
     * {@link S3#copyObject(String, String, String, String)}
     * </p>
     */
    @Test
    public void testCopyObject() {
        System.out.println(s3Instance.copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
    }

    /**
     * <p>
     * {@link S3#deleteObject(String, String)}
     * </p>
     */
    @Test
    public void testDeleteObject() {
        System.out.println(s3Instance.deleteObject(destinationBucketName, destinationKey));
    }

    /**
     * <p>
     * {@link S3#deleteObjects(String, String[])}
     * </p>
     */
    @Test
    public void testDeleteObjectsWithKeys() {
        System.out.println(s3Instance.deleteObjects(sourceBucketName, keys));
    }

    /**
     * <p>
     * {@link S3#moveObject(String, String, String, String)}
     * </p>
     */
    @Test
    public void testMoveObject() {
        System.out.println(s3Instance.moveObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
    }

    /**
     * <p>
     * {@link S3#getBucketAcl(String)}
     * </p>
     */
    @Test
    public void testGetBucketAcl() {
        AccessControlList acl = s3Instance.getBucketAcl(sourceBucketName);
        List<Grant> grants = acl.getGrantsAsList();
        for (Grant grant : grants) {
            System.out.format(" * [%s]: %s\n", grant.getGrantee().getIdentifier(),
                    grant.getPermission().toString());
        }
    }

}
