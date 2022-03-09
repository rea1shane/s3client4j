package io.shane.provider;

import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.transfer.ObjectTaggingProvider;
import com.amazonaws.services.s3.transfer.UploadContext;

// TODO 待实现
public class CustomObjectTaggingProvider implements ObjectTaggingProvider {

    @Override
    public ObjectTagging provideObjectTags(UploadContext uploadContext) {
        return null;
    }

}
