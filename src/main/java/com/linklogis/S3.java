package com.linklogis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;

import java.util.List;

public class S3 {

    final static AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    /**
     * 列出所有的桶
     *
     * @return 返回所有的桶
     */
    public static List<Bucket> listBuckets() {
        return s3.listBuckets();
    }

    /**
     * 列出指定的桶
     *
     * @param bucketName 桶的名称
     * @return 返回指定名称的桶，没有的话返回 null
     */
    public static Bucket getBucket(String bucketName) {
        Bucket targetBucket = null;
        List<Bucket> buckets = listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                targetBucket = b;
            }
        }
        return targetBucket;
    }

    /**
     * 创建桶
     *
     * @param bucketName 桶的名称
     * @return 返回创建的指定名称的桶，如果桶已存在则会返回该桶并提示已存在
     */
    public static Bucket createBucket(String bucketName) {
        Bucket b = null;
        if (s3.doesBucketExistV2(bucketName)) {
            System.out.format("Bucket %s already exists.\n", bucketName);
            b = getBucket(bucketName);
        } else {
            try {
                b = s3.createBucket(bucketName);
            } catch (AmazonS3Exception e) {
                System.err.println(e.getErrorMessage());
            }
        }
        return b;
    }

    /**
     * 删除指定的桶
     *
     * @param bucketName 桶的名称
     */
    public static boolean deleteBucket(String bucketName) {
        System.out.println("Deleting S3 bucket: " + bucketName);
        try {
            System.out.println(" - removing objects from bucket");
            ObjectListing objectListing = s3.listObjects(bucketName);
            while (true) {
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    s3.deleteObject(bucketName, summary.getKey());
                }

                // more objectListing to retrieve?
                if (objectListing.isTruncated()) {
                    objectListing = s3.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            System.out.println(" - removing versions from bucket");
            VersionListing versionListing = s3.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                for (S3VersionSummary vs : versionListing.getVersionSummaries()) {
                    s3.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }

                // more versionListing to retrieve?
                if (versionListing.isTruncated()) {
                    versionListing = s3.listNextBatchOfVersions(versionListing);
                } else {
                    break;
                }
            }

            System.out.println(" OK, bucket ready to delete!");
            s3.deleteBucket(bucketName);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return false;
        }
        System.out.println("Done!");
        return true;
    }

}
