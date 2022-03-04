package io.shane.storage;

import io.shane.pojo.Workspace;

public class ObjectStorageTools {

    public static String getRawPath(Workspace workspace) {
        return String.format("workspace/%s/data/%s", getWorkspaceToken(workspace));
    }

    public static String getCookedPath(Workspace workspace) {
        return String.format("workspace/%s/data/raw", getWorkspaceToken(workspace));
    }

    private static String getWorkspaceToken(Workspace workspace) {
        return workspace.getId();
    }

}
