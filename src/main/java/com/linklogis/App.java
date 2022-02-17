package com.linklogis;

import com.amazonaws.services.s3.model.Bucket;

import java.util.List;

public class App {
    public static void main(String[] args) {
        List<Bucket> buckets = S3.listBucket();
        System.out.println("Your Amazon S3 buckets are:");
        for (Bucket b : buckets) {
            System.out.println("* " + b.getName());
        }
    }
}
