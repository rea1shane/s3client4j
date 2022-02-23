package com.linklogis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.linklogis.override.ListObjectsRequest;
import com.linklogis.override.PutObjectRequest;

import java.io.InputStream;
import java.util.List;

public class S3 {

    private final AmazonS3 s3;

    /**
     * <p>
     * 使用默认配置的 S3 client
     * </p>
     */
    public S3() {
        this.s3 = AmazonS3ClientBuilder.standard().build();
    }

    /**
     * <p>
     * 使用自定义的 s3 client
     * </p>
     *
     * @param s3 自定义 s3 client
     */
    public S3(AmazonS3 s3) {
        this.s3 = s3;
    }

    /**
     * <p>
     * 列出所有的桶
     * </p>
     *
     * @return 桶列表
     */
    public List<Bucket> listBuckets() {
        return this.s3.listBuckets();
    }

    /**
     * <p>
     * 获取指定的桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 指定名称的桶，没有的话返回 null
     */
    public Bucket getBucket(String bucketName) {
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
     * <p>
     * 检查 bucket 是否存在
     * </p>
     *
     * @param bucketName 桶的名称
     * @return true：存在 / false：不存在
     */
    public boolean checkBucketExist(String bucketName) {
        return this.s3.doesBucketExistV2(bucketName);
    }

    /**
     * <p>
     * 创建桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 执行结果
     */
    public String createBucket(String bucketName) {
        String msg = "OK";
        try {
            System.out.printf("Creating S3 bucket [%s]...\n", bucketName);
            this.s3.createBucket(bucketName);
            System.out.println("Done!");
        } catch (AmazonServiceException e) {
            msg = e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        }
        return msg;
    }

    /**
     * <p>
     * 删除指定的桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 执行结果
     */
    public String deleteBucket(String bucketName) {
        String msg = "OK";
        try {
            System.out.printf("Deleting S3 bucket [%s]:\n", bucketName);
            System.out.println(" - removing objects from bucket...");
            ObjectListing objectListing = listObjects(bucketName);
            while (true) {
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    this.s3.deleteObject(bucketName, summary.getKey());
                }

                // more objectListing to retrieve?
                if (objectListing.isTruncated()) {
                    objectListing = this.s3.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }
            System.out.println(" - done!");

            System.out.println(" - removing versions from bucket...");
            VersionListing versionListing = this.s3.listVersions(new ListVersionsRequest().withBucketName(bucketName));
            while (true) {
                for (S3VersionSummary vs : versionListing.getVersionSummaries()) {
                    this.s3.deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }

                // more versionListing to retrieve?
                if (versionListing.isTruncated()) {
                    versionListing = this.s3.listNextBatchOfVersions(versionListing);
                } else {
                    break;
                }
            }
            System.out.println(" - done!");

            System.out.println(" - deleting bucket...");
            this.s3.deleteBucket(bucketName);
            System.out.println("Done!");
        } catch (AmazonServiceException e) {
            msg = e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        }
        return msg;
    }

    /**
     * <p>
     * 获取指定桶中的对象信息
     * </p>
     * <p>
     * 如果想要添加更多请求配置，请调用 {@link S3#listObjects(ListObjectsRequest)}
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 一个 ObjectListing 对象，该对象提供有关存储桶中对象的信息
     */
    public ObjectListing listObjects(String bucketName) {
        return listObjects(new ListObjectsRequest(bucketName, null, null, null, null));
    }

    /**
     * <p>
     * 获取指定桶、指定前缀中的对象信息
     * </p>
     * <p>
     * 如果想要添加更多请求配置，请调用 {@link S3#listObjects(ListObjectsRequest)}
     * </p>
     *
     * @param bucketName 桶的名称
     * @param prefix     路径前缀
     * @return 一个 ObjectListing 对象，该对象提供有关存储桶中对象的信息
     */
    public ObjectListing listObjects(String bucketName, String prefix) {
        return listObjects(new ListObjectsRequest(bucketName, prefix, null, null, null));
    }

    /**
     * <p>
     * 获取桶中的对象信息，通过配置请求参数来筛选 objects
     * </p>
     *
     * @param listObjectsRequest 请求对象，包含列出指定桶中的对象的所有选项
     * @return 一个 ObjectListing 对象，该对象提供有关存储桶中对象的信息
     */
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
        return this.s3.listObjects(listObjectsRequest);
    }

    /**
     * <p>
     * 上传本地文件到指定的桶，并且指定对象的键
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param input      文件流
     * @param metadata   元数据
     * @return 执行结果
     */
    public String putObject(String bucketName, String key, InputStream input, ObjectMetadata metadata) {
        return putObject(new PutObjectRequest(bucketName, key, input, metadata));
    }

    /**
     * <p>
     * 上传文件
     * </p>
     *
     * @param putObjectRequest 请求对象，包含上传对象的所有选项
     * @return 执行结果
     */
    public String putObject(PutObjectRequest putObjectRequest) {
        String msg = "OK";
        try {
            System.out.format("Uploading [%s] to S3 bucket [%s]...\n", putObjectRequest.getKey(), putObjectRequest.getBucketName());
            this.s3.putObject(putObjectRequest);
            System.out.println("Done!");
        } catch (AmazonServiceException e) {
            msg = e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        }
        return msg;
    }

}
