package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;

import java.util.List;

public class App {
    public static void main(String[] args) {
        // create
        if (S3.createBucket("create-by-java-sdk") != null) {
            System.out.println("创建成功或者已经存在");
        }

        // delete
        System.out.println(S3.deleteBucket("create-by-java-sdk"));

        // list
        List<Bucket> buckets = S3.listBuckets();
        System.out.println("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }
}
