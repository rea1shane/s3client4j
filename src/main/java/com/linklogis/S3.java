package com.linklogis;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
     * @param bucketName bucket 的名称
     * @return 返回指定名称的桶，没有的话返回 null
     */
    public static Bucket getBucket(String bucketName) {
        Bucket targetBucket = null;
        List<Bucket> buckets = s3.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                targetBucket = b;
            }
        }
        return targetBucket;
    }

}
