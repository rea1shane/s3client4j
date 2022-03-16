package io.shane;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortIncompleteMultipartUpload;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteVersionRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.SetBucketLifecycleConfigurationRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import io.shane.provider.CustomObjectMetadataProvider;
import io.shane.provider.CustomObjectTagsProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO 操作添加审计，每一次操作都会生成 id，子操作 id 与父操作 id 关联
public class S3 {

    private AmazonS3 s3;
    private TransferManager transferManager;

    /**
     * <p>
     * 使用默认配置的 S3 client
     * </p>
     */
    public S3() {
        setS3(null);
        setTransferManager(null);
    }

    /**
     * <p>
     * 使用自定义的 s3 client
     * </p>
     *
     * @param s3 自定义 s3 client
     */
    public S3(AmazonS3 s3, TransferManager transferManager) {
        setS3(s3);
        setTransferManager(transferManager);
    }

    private void setS3(AmazonS3 s3) {
        this.s3 = s3 == null ? AmazonS3ClientBuilder.standard().build() : s3;
    }

    private void setTransferManager(TransferManager transferManager) {
        this.transferManager = transferManager == null ? TransferManagerBuilder.standard().build() : transferManager;
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
            do {
                for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
                    deleteObject(bucketName, summary.getKey());
                }
                objectListing = listNextBatchOfObjects(objectListing);
            } while (objectListing != null);
            System.out.println(" - done!");

            System.out.println(" - removing versions from bucket...");
            VersionListing versionListing = listVersions(new ListVersionsRequest().withBucketName(bucketName));
            do {
                for (S3VersionSummary vs : versionListing.getVersionSummaries()) {
                    deleteVersion(bucketName, vs.getKey(), vs.getVersionId());
                }
                versionListing = listNextBatchOfVersions(versionListing);
            } while (versionListing != null);
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
     * <p>
     * 注意：此方法会对结果进行分页，要获取所有结果请调用 {@link S3#listNextBatchOfObjects(ObjectListing)}
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
     * <p>
     * 注意：此方法会对结果进行分页，要获取所有结果请调用 {@link S3#listNextBatchOfObjects(ObjectListing)}
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
     * <p>
     * 注意：此方法会对结果进行分页，要获取所有结果请调用 {@link S3#listNextBatchOfObjects(ObjectListing)}
     * </p>
     *
     * @param listObjectsRequest 请求对象，包含列出指定桶中的对象的所有选项
     * @return 一个 ObjectListing 对象，该对象提供有关存储桶中对象的信息
     */
    private ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
        return this.s3.listObjects(listObjectsRequest);
    }

    /**
     * <p>
     * 列出 objectListing 的下一个分页，如果已经是最后一个分页则返回 null
     * </p>
     *
     * @param objectListing 要获取下一个分页的 ObjectListing 对象
     * @return objectListing 的下一个分页
     */
    public ObjectListing listNextBatchOfObjects(ObjectListing objectListing) {
        ObjectListing nextBatch = null;
        if (objectListing.isTruncated()) {
            nextBatch = this.s3.listNextBatchOfObjects(objectListing);
        }
        return nextBatch;
    }

    /**
     * <p>
     * 列出启用版本控制的存储桶中的对象
     * </p>
     * <p>
     * 注意：此方法会对结果进行分页，要获取所有结果请调用 {@link S3#listNextBatchOfVersions(VersionListing)}
     * </p>
     *
     * @param bucketName 桶的名称
     * @param prefix     检索前缀或者过滤字符串
     * @return 一个 VersionListing 对象
     */
    public VersionListing listVersions(String bucketName, String prefix) {
        return listVersions(new ListVersionsRequest(bucketName, prefix, null, null, null, null));
    }

    /**
     * <p>
     * 列出启用版本控制的存储桶中的对象
     * </p>
     * <p>
     * 注意：此方法会对结果进行分页，要获取所有结果请调用 {@link S3#listNextBatchOfVersions(VersionListing)}
     * </p>
     *
     * @param listVersionsRequest 请求对象，包含列出指定桶中的对象的所有选项
     * @return 一个 VersionListing 对象
     */
    private VersionListing listVersions(ListVersionsRequest listVersionsRequest) {
        return this.s3.listVersions(listVersionsRequest);
    }

    /**
     * <p>
     * 列出 versionListing 的下一个分页，如果已经是最后一个分页则返回 null
     * </p>
     *
     * @param versionListing 要获取下一个分页的 VersionListing 对象
     * @return versionListing 的下一个分页
     */
    public VersionListing listNextBatchOfVersions(VersionListing versionListing) {
        VersionListing nextBatch = null;
        if (versionListing.isTruncated()) {
            nextBatch = this.s3.listNextBatchOfVersions(versionListing);
        }
        return nextBatch;
    }

    /**
     * <p>
     * 删除桶中对象的版本
     * </p>
     * <p>
     * 与 {@link S3#deleteObjects(DeleteObjectsRequest)} 不同的是，此操作会删除干净对象的版本信息，{@link S3#deleteObjects(DeleteObjectsRequest)} 只是标记删除了对象或者对象版本
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param versionId  对象的版本 ID
     * @return 操作结果
     */
    public String deleteVersion(String bucketName, String key, String versionId) {
        return deleteVersion(new DeleteVersionRequest(bucketName, key, versionId));
    }

    /**
     * <p>
     * 删除桶中对象的版本
     * </p>
     * <p>
     * 与 {@link S3#deleteObjects(DeleteObjectsRequest)} 不同的是，此操作会删除干净对象的版本信息，{@link S3#deleteObjects(DeleteObjectsRequest)} 只是标记删除了对象或者对象版本
     * </p>
     *
     * @param deleteVersionRequest 请求对象，包含删除对象版本的所有选项
     * @return 操作结果
     */
    public String deleteVersion(DeleteVersionRequest deleteVersionRequest) {
        String msg = "OK";
        try {
            System.out.println("Deleting version...");
            this.s3.deleteVersion(deleteVersionRequest);
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
     * 上传文件
     * </p>
     *
     * @param bucketName  桶的名称
     * @param key         对象的键
     * @param inputStream 文件流
     * @param metadata    元数据
     * @return 操作结果
     */
    public String putObject(String bucketName, String key, InputStream inputStream, ObjectMetadata metadata) {
        return putObject(new PutObjectRequest(bucketName, key, inputStream, metadata));
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
            msg = "Amazon service error: " + e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        } catch (IOException e) {
            msg = "IO error: " + e.getMessage();
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
     * @param key       对象键
     * @param versionId 版本 ID，如果要删除对象的所有版本的话传 null
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
        keyWithVersionIds.forEach((key, versionId) -> keyVersions.add(new DeleteObjectsRequest.KeyVersion(key, versionId)));
        return keyVersions;
    }

    /**
     * <p>
     * 删除对象
     * </p>
     *
     * @param deleteObjectsRequest 请求对象，包含删除对象的所有选项
     * @return 操作结果
     */
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

    /**
     * <p>
     * 使用 InputStream，通过 TransferManager 上传文件
     * </p>
     *
     * @param bucketName  桶的名称
     * @param key         对象的键
     * @param inputStream 文件流
     * @return upload 对象
     */
    public Upload uploadFile(String bucketName, String key, InputStream inputStream) {
        return uploadFile(bucketName, key, inputStream, null, null);
    }

    /**
     * <p>
     * 使用 InputStream，通过 TransferManager 上传文件
     * </p>
     *
     * @param bucketName  桶的名称
     * @param key         对象的键
     * @param inputStream 文件流
     * @param metadata    元数据
     * @param tags        标签
     * @return upload 对象
     */
    public Upload uploadFile(String bucketName, String key, InputStream inputStream, ObjectMetadata metadata, ObjectTagging tags) {
        return upload(new PutObjectRequest(bucketName, key, inputStream, metadata).withTagging(tags));
    }

    /**
     * <p>
     * 使用 File，通过 TransferManager 上传文件
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param file       文件
     * @return upload 对象
     */
    public Upload uploadFile(String bucketName, String key, File file) {
        return uploadFile(bucketName, key, file, null, null);
    }

    /**
     * <p>
     * 使用 File，通过 TransferManager 上传文件
     * </p>
     *
     * @param bucketName 桶的名称
     * @param key        对象的键
     * @param file       文件
     * @param metadata   元数据
     * @param tags       标签
     * @return upload 对象
     */
    public Upload uploadFile(String bucketName, String key, File file, ObjectMetadata metadata, ObjectTagging tags) {
        return upload(new PutObjectRequest(bucketName, key, file).withMetadata(metadata).withTagging(tags));
    }

    /**
     * <p>
     * 通过 TransferManager 上传对象
     * </p>
     *
     * @param putObjectRequest 请求对象，包含上传对象的所有选项
     * @return upload 对象
     */
    private Upload upload(PutObjectRequest putObjectRequest) {
        Upload uploadObject = null;
        try {
            uploadObject = this.transferManager.upload(putObjectRequest);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
        return uploadObject;
    }

    /**
     * <p>
     * 使用 List<File>，通过 TransferManager 上传多个文件
     * </p>
     *
     * @param bucketName 桶的名称
     * @param prefix     上传路径
     * @param directory  本地文件的路径文件的路径
     * @param files      待上传的文件列表
     * @return multipleFileUpload 对象
     */
    public MultipleFileUpload uploadFileList(String bucketName, String prefix, File directory, List<File> files) {
        return uploadFileList(bucketName, prefix, directory, files, null, null);
    }

    /**
     * <p>
     * 使用 List<File>，通过 TransferManager 上传多个文件
     * </p>
     *
     * @param bucketName       桶的名称
     * @param prefix           上传路径
     * @param directory        本地文件的路径文件的路径
     * @param files            待上传的文件列表
     * @param metadataProvider 用于赋予不同文件元数据
     * @param tagsProvider     用于赋予不同文件标签
     * @return multipleFileUpload 对象
     */
    public MultipleFileUpload uploadFileList(String bucketName, String prefix, File directory, List<File> files, CustomObjectMetadataProvider metadataProvider, CustomObjectTagsProvider tagsProvider) {
        return this.transferManager.uploadFileList(bucketName, prefix, directory, files, metadataProvider, tagsProvider);
    }

    /**
     * <p>
     * 通过 TransferManager 拷贝对象
     * </p>
     *
     * @param sourceBucketName      源桶名称
     * @param sourceKey             源对象键
     * @param destinationBucketName 目标桶名称
     * @param destinationKey        目标对象键
     * @return copy 对象
     */
    public Copy copy(String sourceBucketName, String sourceKey, String destinationBucketName, String destinationKey) {
        return copy(new CopyObjectRequest(sourceBucketName, sourceKey, destinationBucketName, destinationKey));
    }

    /**
     * <p>
     * 通过 TransferManager 拷贝对象
     * </p>
     *
     * @param copyObjectRequest 请求对象，包含拷贝对象的所有选项
     * @return copy 对象
     */
    private Copy copy(CopyObjectRequest copyObjectRequest) {
        Copy copy = null;
        try {
            copy = this.transferManager.copy(copyObjectRequest);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
        }
        return copy;
    }

    /**
     * <p>
     * 获取指定桶的版本控制开关状态
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 版本控制状态，关闭返回 Off，开启返回 Enabled，暂停返回 Suspended {@link BucketVersioningConfiguration}
     */
    public String getBucketVersioningStatus(String bucketName) {
        BucketVersioningConfiguration conf = this.s3.getBucketVersioningConfiguration(bucketName);
        return conf.getStatus();
    }

    /**
     * <p>
     * 开启或暂停指定桶的版本控制
     * </p>
     * <p>
     * 版本控制有三种状态，禁用 / 开启 / 暂停，创建一个 s3 桶的时候默认是禁用状态，可以通过配置开启版本控制，开启后就不能禁用版本控制了，只能暂停版本控制，此操作会暂停创建所有操作的对象版本，但会保留任何现有的对象版本。
     * </p>
     *
     * @param bucketName 桶的名称
     * @param enable     是否开启版本控制
     * @return 操作结果
     */
    public String switchBucketVersioningStatus(String bucketName, boolean enable) {
        String msg = "OK";
        System.out.println("Switching bucket versioning status...");
        String status = getBucketVersioningStatus(bucketName);
        boolean statusBool;
        switch (status) {
            case "Off":
            case "Suspended":
                statusBool = false;
                break;
            case "Enabled":
                statusBool = true;
                break;
            default:
                msg = "Unknown versioning status: " + status;
                return msg;
        }
        if (enable == statusBool) {
            String statusString = enable ? "enable" : "off or suspended";
            msg = "Already " + statusString + ", no change.";
            System.out.println(msg);
        } else {
            try {
                BucketVersioningConfiguration configuration = new BucketVersioningConfiguration();
                configuration = enable ? configuration.withStatus("Enabled") : configuration.withStatus("Suspended");
                SetBucketVersioningConfigurationRequest setBucketVersioningConfigurationRequest = new SetBucketVersioningConfigurationRequest(bucketName, configuration);
                this.s3.setBucketVersioningConfiguration(setBucketVersioningConfigurationRequest);
                System.out.println("Done!");
            } catch (AmazonServiceException e) {
                msg = e.getErrorMessage();
                System.err.println(msg);
                System.err.println("Failure!");
            }
        }
        return msg;
    }

    /**
     * <p>
     * 获取桶的生命周期配置
     * </p>
     *
     * @param bucketName 桶的名称
     * @return 桶的生命周期配置文件
     */
    public BucketLifecycleConfiguration getBucketLifecycleConfiguration(String bucketName) {
        return this.s3.getBucketLifecycleConfiguration(bucketName);
    }

    /**
     * <p>
     * 该规则指示 Amazon S3 中止在启动后没有在指定天数内完成的分段上传。当超过设置的时间限制时，Amazon S3 将中止上传，然后删除未完成的上传数据。
     * </p>
     *
     * @param bucketName          桶的名称
     * @param daysAfterInitiation 指示生命周期从开始到中止不完整的多部分上传必须经过的天数
     * @return 操作结果
     */
    public String setBucketLifecycleAbortIncompleteMultipartUpload(String bucketName, int daysAfterInitiation) {
        AbortIncompleteMultipartUpload abortIncompleteMultipartUpload = new AbortIncompleteMultipartUpload();
        abortIncompleteMultipartUpload.setDaysAfterInitiation(daysAfterInitiation);

        BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
                .withId("Abort incomplete multipart upload rule")
                .withStatus(BucketLifecycleConfiguration.ENABLED);
        rule.setAbortIncompleteMultipartUpload(abortIncompleteMultipartUpload);

        BucketLifecycleConfiguration bucketLifecycleConfiguration = getBucketLifecycleConfiguration(bucketName);
        if (bucketLifecycleConfiguration == null) {
            bucketLifecycleConfiguration = new BucketLifecycleConfiguration();
        }
        bucketLifecycleConfiguration.withRules(rule);
        return setBucketLifecycleConfiguration(bucketName, bucketLifecycleConfiguration);
    }

    /**
     * <p>
     * 配置桶的生命周期规则
     * </p>
     *
     * @param bucketName                   桶的名称
     * @param bucketLifecycleConfiguration 桶的生命周期配置文件
     * @return 操作结果
     */
    public String setBucketLifecycleConfiguration(String bucketName, BucketLifecycleConfiguration bucketLifecycleConfiguration) {
        return setBucketLifecycleConfiguration(new SetBucketLifecycleConfigurationRequest(bucketName, bucketLifecycleConfiguration));
    }

    /**
     * <p>
     * 配置桶的生命周期规则
     * </p>
     *
     * @param setBucketLifecycleConfigurationRequest 请求对象，包含设置生命周期规则的所有选项
     * @return 操作结果
     */
    private String setBucketLifecycleConfiguration(SetBucketLifecycleConfigurationRequest setBucketLifecycleConfigurationRequest) {
        String msg = "OK";
        try {
            System.out.println("Setting bucket lifecycle configuration...");
            this.s3.setBucketLifecycleConfiguration(setBucketLifecycleConfigurationRequest);
            System.out.println("Done!");
        } catch (AmazonServiceException e) {
            msg = e.getErrorMessage();
            System.err.println(msg);
            System.err.println("Failure!");
        }
        return msg;
    }

}
