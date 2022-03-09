package io.shane.provider;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;

import java.io.File;

// TODO 待实现
public class CustomObjectMetadataProvider implements ObjectMetadataProvider {

    @Override
    public void provideObjectMetadata(File file, ObjectMetadata metadata) {

    }

}
