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

import java.io.File;
import java.nio.file.Paths;
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
     * 检查 bucket 是否存在
     *
     * @param bucketName 桶的名称
     * @return true：存在 / false：不存在
     */
    public static boolean checkBucketExist(String bucketName) {
        return s3.doesBucketExistV2(bucketName);
    }

    /**
     * 创建桶
     *
     * @param bucketName 桶的名称
     * @return true：创建成功 / false：创建异常
     */
    public static boolean createBucket(String bucketName) {
        System.out.println("Creating S3 bucket: " + bucketName);
        try {
            s3.createBucket(bucketName);
        } catch (AmazonS3Exception e) {
            System.err.println(e.getErrorMessage());
            return false;
        }
        System.out.println("Done!");
        return true;
    }

    /**
     * 在创建桶前检查桶是否存在。如果存在的话，返回该桶；如果不存在的话，创建桶，并且返回该桶
     *
     * @param bucketName 桶的名称
     * @return 返回创建的指定名称的桶，如果桶已存在则会返回该桶并提示已存在，返回 null 代表创建异常
     */
    public static Bucket checkExistAndCreateBucket(String bucketName) {
        if (checkBucketExist(bucketName)) {
            System.out.format("Bucket %s already exists.\n", bucketName);
        } else {
            createBucket(bucketName);
        }
        return getBucket(bucketName);
    }

    /**
     * 删除指定的桶
     *
     * @param bucketName 桶的名称
     * @return true：删除成功 / false：删除异常
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

    /**
     * 上传本地文件到指定的桶
     * 以本地文件的名称作为 S3 中对象的名称
     * 不管本地文件的路径差异，只要将相同名字的文件上传到同一个桶中，后上传的文件就会覆盖先上传的
     *
     * @param bucketName 桶的名称
     * @param filePath   上传的本地文件路径
     * @return true：上传成功 / false：上传异常
     */
    public static boolean putObject(String bucketName, String filePath) {
        System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucketName);
        String keyName = Paths.get(filePath).getFileName().toString();
        try {
            s3.putObject(bucketName, keyName, new File(filePath));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return false;
        }
        System.out.println("Done!");
        return true;
    }

}
