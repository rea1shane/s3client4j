package io.shane.storage;

public enum StoragePartition implements StoragePartitionInterface {
    RAW("raw", StoragePartitionType.DATA),
    COOKED("cooked", StoragePartitionType.DATA),
    FEATURE("feature", StoragePartitionType.DATA),
    ;

    private final String path;
    private final StoragePartitionType type;

    StoragePartition(String path, StoragePartitionType type) {
        this.path = path;
        this.type = type;
    }

    public StoragePartitionType getType() {
        return this.type;
    }

    @Override
    public String getPath() {
        return String.format("%s/%s", this.type.getPath(), this.path);
    }
}
