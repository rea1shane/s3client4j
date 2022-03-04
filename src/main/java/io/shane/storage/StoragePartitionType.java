package io.shane.storage;

public enum StoragePartitionType implements StoragePartitionInterface {

    TMP("tmp"),
    DATA("data"),
    MODEL("model"),
    ;

    private final String path;

    StoragePartitionType(String path) {
        this.path = path;
    }

    @Override
    public String getPath() {
        return this.path;
    }

}
