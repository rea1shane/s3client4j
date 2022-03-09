package io.shane.provider;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.ObjectMetadataProvider;

import java.io.File;
import java.util.Map;

public class CustomObjectMetadataProvider implements ObjectMetadataProvider {

    private final Map<File, Map<String, String>> fileMetadataMap;

    public CustomObjectMetadataProvider(Map<File, Map<String, String>> fileMetadataMap) {
        this.fileMetadataMap = fileMetadataMap;
    }

    @Override
    public void provideObjectMetadata(File file, ObjectMetadata metadata) {
        if (this.fileMetadataMap.containsKey(file)) {
            metadata.setUserMetadata(this.fileMetadataMap.get(file));
        }
    }

}
