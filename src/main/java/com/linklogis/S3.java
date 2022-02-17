package com.linklogis;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;

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

}
