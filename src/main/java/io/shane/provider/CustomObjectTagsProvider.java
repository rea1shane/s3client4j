package io.shane.provider;

import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.services.s3.transfer.ObjectTaggingProvider;
import com.amazonaws.services.s3.transfer.UploadContext;

import java.io.File;
import java.util.List;
import java.util.Map;

public class CustomObjectTagsProvider implements ObjectTaggingProvider {

    private final Map<File, List<Tag>> fileTagsMap;

    public CustomObjectTagsProvider(Map<File, List<Tag>> fileTagsMap) {
        this.fileTagsMap = fileTagsMap;
    }

    @Override
    public ObjectTagging provideObjectTags(UploadContext uploadContext) {
        File file = uploadContext.getFile();
        if (this.fileTagsMap.containsKey(file)) {
            return new ObjectTagging(this.fileTagsMap.get(file));
        }
        return null;
    }

}
