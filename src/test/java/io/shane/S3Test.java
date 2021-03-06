package io.shane;

import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.Upload;
import io.shane.provider.CustomObjectMetadataProvider;
import io.shane.provider.CustomObjectTagsProvider;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
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
    String picFilePath = "/Users/shane/Desktop/DSCF4585.RAF";
    String picKey = "DSCF4585.RAF";
    String dir = "/Users/shane/Desktop/AMBARI";
    String file1 = dir + "/PROMETHEUS/metainfo.xml";
    String file2 = dir + "/PROMETHEUS/quicklinks/quicklinks.json";

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
        ObjectListing objectListing = s3Instance.listObjects(sourceBucketName, "dir_test/PROMETHEUS/configuration/azkabanexporter-env.xml");
        System.out.format("Objects in S3 bucket [%s] with prefix [%s] are:\n", sourceBucketName, prefix);
        for (S3ObjectSummary summary : objectListing.getObjectSummaries()) {
            System.out.println("* " + summary.getKey());
        }
    }

    /**
     * <p>
     * {@link S3#listVersions(String, String)}
     * </p>
     * <p>
     * {@link S3#listNextBatchOfVersions(VersionListing)}
     * </p>
     */
    @Test
    public void testListVersions() {
        VersionListing versionListing = s3Instance.listVersions(sourceBucketName, null);
        for (S3VersionSummary versionSummary : versionListing.getVersionSummaries()) {
            // TODO ???????????????????????????????????????
            System.out.println(versionSummary.isDeleteMarker() + " " + versionSummary.getLastModified().toString());
            System.out.println(versionSummary.getKey() + " : " + versionSummary.getVersionId());
        }
        System.out.println(s3Instance.listNextBatchOfVersions(versionListing));
    }

    /**
     * <p>
     * {@link S3#deleteVersion(String, String, String)}
     * </p>
     */
    @Test
    public void testDeleteVersion() {
        s3Instance.deleteVersion(sourceBucketName, "sample_07.csv", "i04N0U0J1J0U7ezRRII7Bz3fglWuWBak");
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
     * {@link S3#getObject(String, String, String)}
     * </p>
     * <p>
     * {@link S3#downloadObject(S3Object, OutputStream)}
     * </p>
     */
    @Test
    public void testDownloadObjectWithVersion() throws FileNotFoundException {
        FileOutputStream outputStream = new FileOutputStream(sourceKey);
        S3Object s3Object = s3Instance.getObject(sourceBucketName, sourceKey, "YzFa8VNpRJS.r76mAO8Hjf.df0JKUvEA");
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
        System.out.println(s3Instance.deleteObject(sourceBucketName, sourceKey));
    }

    /**
     * <p>
     * {@link S3#deleteObject(String, String, String)}
     * </p>
     */
    @Test
    public void testDeleteObjectWithVersion() {
        System.out.println(s3Instance.deleteObject(sourceBucketName, sourceKey, "YzFa8VNpRJS.r76mAO8Hjf.df0JKUvEA"));
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
     * {@link S3#deleteObjects(String, Map)}
     * </p>
     */
    @Test
    public void testDeleteObjectsWithKeysAndVersion() {
        Map<String, String> keyWithVersionIds = new HashMap<>();
        keyWithVersionIds.put("dir_test/.DS_Store", "ldAf_sOvH4eiMLpPc7vhacegIyst_kIX");
        keyWithVersionIds.put("dir_test/channel/.DS_Store", "VRoqMBWUyD.Ux5nZ2bScA1588UGopbVO");
        keyWithVersionIds.put("dir_test/PROMETHEUS/.DS_Store", null);
        System.out.println(s3Instance.deleteObjects(sourceBucketName, keyWithVersionIds));
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
     * {@link S3#uploadFile(String, String, InputStream, ObjectMetadata, ObjectTagging)}
     * </p>
     */
    @Test
    public void testUploadFileWithStream() throws IOException {
        InputStream input = new FileInputStream(picFilePath);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("method", "transfer_manager_with_stream");
        objectMetadata.setUserMetadata(metadata);

        ArrayList<Tag> tags = new ArrayList<>();
        Tag tag1 = new Tag("from", "mac");
        Tag tag2 = new Tag("auth", "shane");
        Tag tag3 = new Tag("method", "transfer_manager_with_stream");
        Tag tag4 = new Tag("version", "new_version");
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        tags.add(tag4);
        ObjectTagging objectTagging = new ObjectTagging(tags);

        Upload upload = s3Instance.uploadFile(sourceBucketName, picKey, input, objectMetadata, objectTagging);
        System.out.println(TransferManagerProgress.waitForCompletion(upload));
        input.close();
    }

    /**
     * <p>
     * {@link S3#uploadFile(String, String, File, ObjectMetadata, ObjectTagging)}
     * </p>
     */
    @Test
    public void testUploadFileWithFile() {
        File file = new File(picFilePath);

        ObjectMetadata objectMetadata = new ObjectMetadata();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("method", "transfer_manager_with_file");
        objectMetadata.setUserMetadata(metadata);

        ArrayList<Tag> tags = new ArrayList<>();
        Tag tag1 = new Tag("from", "mac");
        Tag tag2 = new Tag("auth", "shane");
        Tag tag3 = new Tag("method", "transfer_manager_with_file");
        tags.add(tag1);
        tags.add(tag2);
        tags.add(tag3);
        ObjectTagging objectTagging = new ObjectTagging(tags);

        Upload upload = s3Instance.uploadFile(sourceBucketName, picKey, file, objectMetadata, objectTagging);
        TransferManagerProgress.showTransferProgress(upload);
    }

    /**
     * <p>
     * {@link S3#uploadFileList(String, String, File, List, CustomObjectMetadataProvider, CustomObjectTagsProvider)}
     * </p>
     */
    @Test
    public void testUploadFileList() {
        File d = new File(dir);
        List<File> files = new ArrayList<>();
        listFiles(d, files, true);

        HashMap<File, Map<String, String>> fileMetadataMap = new HashMap<>();
        Map<File, List<Tag>> fileTagsMap = new HashMap<>();

        File metainfo = new File(file1);

        Map<String, String> metainfoMetadata = new HashMap<>();
        metainfoMetadata.put("file_name", "metainfo.xml");
        fileMetadataMap.put(metainfo, metainfoMetadata);

        ArrayList<Tag> metainfoTags = new ArrayList<>();
        Tag metainfoTag1 = new Tag("no", "1");
        Tag metainfoTag2 = new Tag("method", "multiple");
        metainfoTags.add(metainfoTag1);
        metainfoTags.add(metainfoTag2);
        fileTagsMap.put(metainfo, metainfoTags);

        File quicklinks = new File(file2);

        Map<String, String> quicklinksMetadata = new HashMap<>();
        quicklinksMetadata.put("file_name", "quicklinks.json");
        fileMetadataMap.put(quicklinks, quicklinksMetadata);

        ArrayList<Tag> quicklinksTags = new ArrayList<>();
        Tag quicklinksTag1 = new Tag("no", "2");
        Tag quicklinksTag2 = new Tag("method", "multiple");
        quicklinksTags.add(quicklinksTag1);
        quicklinksTags.add(quicklinksTag2);
        fileTagsMap.put(quicklinks, quicklinksTags);

        CustomObjectMetadataProvider customObjectMetadataProvider = new CustomObjectMetadataProvider(fileMetadataMap);
        CustomObjectTagsProvider customObjectTagsProvider = new CustomObjectTagsProvider(fileTagsMap);

        MultipleFileUpload multipleFileUpload = s3Instance.uploadFileList(sourceBucketName, "dir_test", d, files, customObjectMetadataProvider, customObjectTagsProvider);
        TransferManagerProgress.showTransferProgress(multipleFileUpload);
    }

    /**
     * <p>
     * {@link S3#copy(String, String, String, String)}
     * </p>
     */
    @Test
    public void testCopy() {
        Copy copy = s3Instance.copy(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        TransferManagerProgress.showTransferProgress(copy);
    }

    /**
     * <p>
     * {@link S3#getBucketVersioningStatus(String)}
     * </p>
     */
    @Test
    public void testGetBucketVersioningStatus() {
        System.out.println(s3Instance.getBucketVersioningStatus(sourceBucketName));
    }

    /**
     * <p>
     * {@link S3#switchBucketVersioningStatus(String, boolean)}
     * </p>
     */
    @Test
    public void testGetBucketLifecycleConfiguration() {
        BucketLifecycleConfiguration bucketLifecycleConfiguration = s3Instance.getBucketLifecycleConfiguration(sourceBucketName);
        for (BucketLifecycleConfiguration.Rule rule : bucketLifecycleConfiguration.getRules()) {
            System.out.println(rule.getId());
        }
    }

    /**
     * <p>
     * {@link S3#setBucketLifecycleAbortIncompleteMultipartUpload(String, int)}
     * </p>
     */
    @Test
    public void testSetBucketLifecycleAbortIncompleteMultipartUpload() {
        System.out.println(s3Instance.setBucketLifecycleAbortIncompleteMultipartUpload(sourceBucketName, 7));
    }

    /**
     * <p>
     * Lists files in the directory given and adds them to the result list
     * passed in, optionally adding subdirectories recursively.
     * </p>
     */
    private void listFiles(File dir, List<File> results, boolean includeSubDirectories) {
        File[] found = dir.listFiles();
        if (found != null) {
            for (File f : found) {
                if (f.isDirectory()) {
                    if (includeSubDirectories) {
                        listFiles(f, results, includeSubDirectories);
                    }
                } else {
                    results.add(f);
                }
            }
        }
    }

}
