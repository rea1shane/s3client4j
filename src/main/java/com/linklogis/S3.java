package com.linklogis;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.linklogis.override.CopyObjectRequest;
import com.linklogis.override.GetObjectRequest;
import com.linklogis.override.ListObjectsRequest;
import com.linklogis.override.PutObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO 操作添加审计，每一次操作都会生成 id，子操作 id 与父操作 id 关联
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
     * @return 操作结果
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
     * @return 操作结果
     */
    public String deleteBucket(String bucketName) {
        String msg = "OK";
        try {
            System.out.printf("Deleting S3 bucket [%s]:\n", bucketName);
            System.out.println(" - removing objects from bucket...");
            ObjectListing objectListing = listObjects(bucketName);
            while (true) {
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    deleteObject(bucketName, summary.getKey());
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
     * 获取指定桶中的对象信息，只能获取到对象的摘要信息
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
     * 获取指定桶、指定前缀中的对象信息，只能获取到对象的摘要信息
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
     * 获取桶中的对象信息，通过配置请求参数来筛选对象，只能获取到对象的摘要信息
     * </p>
     *
     * @param listObjectsRequest 请求对象，包含列出指定桶中的对象的所有选项
     * @return 一个 ObjectListing 对象，该对象提供有关存储桶中对象的信息
     */
    private ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
        System.out.println("Params:\n" + listObjectsRequest); // debug
        return this.s3.listObjects(listObjectsRequest);
    }

    /**
     * <p>
     * 上传文件
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param input      文件流
     * @param metadata   元数据
     * @return 操作结果
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
     * @return 操作结果
     */
    private String putObject(PutObjectRequest putObjectRequest) {
        String msg = "OK";
        try {
            System.out.format("Uploading [%s] to S3 bucket [%s]...\n", putObjectRequest.getKey(), putObjectRequest.getBucketName());
            System.out.println("Params:\n" + putObjectRequest); // debug
            this.s3.putObject(putObjectRequest);
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
     * 获取对象的最新版本
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @return S3 对象
     */
    public S3Object getObject(String bucketName, String key) {
        return getObject(new GetObjectRequest(bucketName, key));
    }

    /**
     * <p>
     * 获取对象的指定版本，需要在 S3 中开启版本控制
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param versionId  对象的版本 ID
     * @return S3 对象
     */
    public S3Object getObject(String bucketName, String key, String versionId) {
        return getObject(new GetObjectRequest(bucketName, key, versionId));
    }

    /**
     * <p>
     * 获取对象
     * </p>
     *
     * @param getObjectRequest 请求对象，包含获取对象的所有选项
     * @return S3 对象
     */
    private S3Object getObject(GetObjectRequest getObjectRequest) {
        S3Object s3Object = null;
        try {
            System.out.println("Params:\n" + getObjectRequest); // debug
            s3Object = this.s3.getObject(getObjectRequest);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
        return s3Object;
    }

    /**
     * <p>
     * 下载对象
     * </p>
     *
     * @param s3Object     S3 对象
     * @param outputStream 文件输出流
     * @return 操作结果
     */
    public String downloadObject(S3Object s3Object, OutputStream outputStream) {
        String msg = "OK";
        try {
            if (s3Object == null) {
                return "The specified object does not exist.";
            }
            System.out.format("Downloading [%s] from S3 bucket [%s]...\n", s3Object.getKey(), s3Object.getBucketName());
            S3ObjectInputStream inputStream = s3Object.getObjectContent();
            byte[] readBuf = new byte[1024];
            int readLen;
            while ((readLen = inputStream.read(readBuf)) > 0) {
                outputStream.write(readBuf, 0, readLen);
            }
            inputStream.close();
            outputStream.close();
            System.out.println("Done!");
        } catch (AmazonServiceException e) {
            msg = e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        } catch (IOException e) {
            msg = e.getMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        }
        return msg;
    }

    /**
     * <p>
     * 拷贝对象
     * </p>
     *
     * @param sourceBucketName      源桶名称
     * @param sourceKey             源对象键
     * @param destinationBucketName 目标桶名称
     * @param destinationKey        目标对象键
     * @return 操作结果
     */
    public String copyObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) {
        return copyObject(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
    }

    /**
     * <p>
     * 拷贝对象
     * </p>
     *
     * @param copyObjectRequest 请求对象，包含拷贝对象的所有选项
     * @return 操作结果
     */
    private String copyObject(CopyObjectRequest copyObjectRequest) {
        String msg = "OK";
        try {
            System.out.format("Copying object, from [%s] / [%s] to [%s] / [%s]...\n", copyObjectRequest.getSourceBucketName(), copyObjectRequest.getSourceKey(), copyObjectRequest.getDestinationBucketName(), copyObjectRequest.getDestinationKey());
            this.s3.copyObject(copyObjectRequest);
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
     * 删除指定对象
     * </p>
     *
     * @param bucketName 桶名称
     * @param key        对象键
     * @return 操作结果
     */
    public String deleteObject(String bucketName, String key) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = convert2KeyVersion(key, null);
        return deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keyVersions));
    }

    /**
     * <p>
     * 删除指定对象的指定版本
     * </p>
     *
     * @param bucketName 桶名称
     * @param key        对象键
     * @param versionId  版本 ID
     * @return 操作结果
     */
    // TODO 待测试
    public String deleteObject(String bucketName, String key, String versionId) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = convert2KeyVersion(key, versionId);
        return deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keyVersions));
    }

    /**
     * <p>
     * 删除一组对象
     * </p>
     *
     * @param bucketName 桶名称
     * @param keys       对象键组成的数组
     * @return 操作结果
     */
    public String deleteObjects(String bucketName, String[] keys) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = convert2KeyVersion(keys);
        return deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keyVersions));
    }

    /**
     * <p>
     * 删除一组对象的指定版本
     * </p>
     *
     * @param bucketName        桶名称
     * @param keyWithVersionIds 对象的键与版本 ID 组成的字典
     * @return 操作结果
     */
    public String deleteObjects(String bucketName, Map<String, String> keyWithVersionIds) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = convert2KeyVersion(keyWithVersionIds);
        return deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(keyVersions));
    }


    /**
     * @param key 对象键
     * @return {@link DeleteObjectsRequest.KeyVersion} 列表
     */
    private List<DeleteObjectsRequest.KeyVersion> convert2KeyVersion(String key, String versionId) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = new ArrayList<>();
        keyVersions.add(new DeleteObjectsRequest.KeyVersion(key, versionId));
        return keyVersions;
    }

    /**
     * @param keys 对象键组成的数组
     * @return {@link DeleteObjectsRequest.KeyVersion} 列表
     */
    private List<DeleteObjectsRequest.KeyVersion> convert2KeyVersion(String[] keys) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = new ArrayList<>();
        for (String key : keys) {
            keyVersions.add(new DeleteObjectsRequest.KeyVersion(key));
        }
        return keyVersions;
    }

    /**
     * @param keyWithVersionIds 对象的键与版本 ID 组成的字典
     * @return {@link DeleteObjectsRequest.KeyVersion} 列表
     */
    private List<DeleteObjectsRequest.KeyVersion> convert2KeyVersion(Map<String, String> keyWithVersionIds) {
        List<DeleteObjectsRequest.KeyVersion> keyVersions = new ArrayList<>();
        keyWithVersionIds.forEach((key, versionId) -> {
            keyVersions.add(new DeleteObjectsRequest.KeyVersion(key, versionId));
        });
        return keyVersions;
    }

    private String deleteObjects(DeleteObjectsRequest deleteObjectsRequest) {
        String msg = "OK";
        try {
            System.out.format("Deleting objects from S3 bucket [%s]...\n", deleteObjectsRequest.getBucketName());
            this.s3.deleteObjects(deleteObjectsRequest);
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
     * 移动对象；当源桶与目标桶相同时，也可以被用作重命名对象
     * </p>
     *
     * @param sourceBucketName      源桶名称
     * @param sourceKey             源对象键
     * @param destinationBucketName 目标桶名称
     * @param destinationKey        目标对象键
     * @return 操作结果
     */
    public String moveObject(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) {
        System.out.format("Moving object, from [%s] / [%s] to [%s] / [%s]...\n", sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        String msg = copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        if (msg.equals("OK")) {
            msg = deleteObject(sourceBucketName, sourceKey);
        }
        if (msg.equals("OK")) {
            System.out.println("Done!");
        } else {
            System.err.println("Failure!");
        }
        return msg;
    }

}
