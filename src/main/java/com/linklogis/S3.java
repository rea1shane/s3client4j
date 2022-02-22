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
import com.linklogis.override.ListObjectsRequest;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

public class S3 {

    final static AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();

    /**
     * <p>
     * 列出所有的桶
     * </p>
     *
     * @return 返回所有的桶
     */
    public static List<Bucket> listBuckets() {
        return s3.listBuckets();
    }

    /**
     * <p>
     * 获取指定的桶
     * </p>
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
     * <p>
     * 检查 bucket 是否存在
     * </p>
     *
     * @param bucketName 桶的名称
     * @return true：存在 / false：不存在
     */
    public static boolean checkBucketExist(String bucketName) {
        return s3.doesBucketExistV2(bucketName);
    }

    /**
     * <p>
     * 创建桶
     * </p>
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
     * <p>
     * 在创建桶前检查桶是否存在。如果存在的话，返回该桶；如果不存在的话，创建桶，并且返回该桶
     * </p>
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
     * <p>
     * 删除指定的桶
     * </p>
     *
     * @param bucketName 桶的名称
     * @return true：删除成功 / false：删除异常
     */
    public static boolean deleteBucket(String bucketName) {
        System.out.println("Deleting S3 bucket: " + bucketName);
        try {
            System.out.println(" - removing objects from bucket");
            ObjectListing objectListing = listObjects(bucketName);
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
     * <p>
     * 上传本地文件到指定的桶，不指定对象的键，以本地文件的名称作为 S3 中对象的键
     * </p>
     *
     * @param bucketName 桶的名称
     * @param filePath   文件路径
     * @return true：上传成功 / false：上传异常
     */
    public static boolean putObject(String bucketName, String filePath) {
        String keyName = Paths.get(filePath).getFileName().toString();
        return putObject(bucketName, keyName, filePath);
    }

    /**
     * <p>
     * 上传本地文件到指定的桶，并且指定对象的键
     * </p>
     *
     * @param bucketName 桶的名称
     * @param keyName    对象的键
     * @param filePath   文件路径
     * @return true：上传成功 / false：上传异常
     */
    public static boolean putObject(String bucketName, String keyName, String filePath) {
        System.out.format("Uploading [%s] to S3 bucket [%s], file path: [%s] ...\n", keyName, bucketName, filePath);
        try {
            s3.putObject(bucketName, keyName, new File(filePath));
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return false;
        }
        System.out.println("Done!");
        return true;
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
    public static ObjectListing listObjects(String bucketName) {
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
    public static ObjectListing listObjects(String bucketName, String prefix) {
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
    public static ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
        return s3.listObjects(listObjectsRequest);
    }

}
