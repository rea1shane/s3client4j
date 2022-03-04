package io.shane;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.transfer.*;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

public class TransferManagerProgress {

    /**
     * <p>
     * 等待 Transfer 完成，捕获发生的任何异常
     * </p>
     *
     * @param transfer 异步上传或者下载对象
     * @return 传输结果
     */
    public static String waitForCompletion(Transfer transfer) {
        String msg = "OK";
        try {
            transfer.waitForCompletion();
        } catch (AmazonServiceException e) {
            msg = "Amazon service error: " + e.getErrorMessage();
        } catch (AmazonClientException e) {
            msg = "Amazon client error: " + e.getMessage();
        } catch (InterruptedException e) {
            msg = "Transfer interrupted: " + e.getMessage();
        }
        return msg;
    }


    /**
     * <p>
     * 在等待传输完成时打印进度条
     * </p>
     *
     * @param transfer 异步上传或者下载对象
     */
    public static void showTransferProgress(Transfer transfer) {
        // transfer 的描述
        System.out.println(transfer.getDescription());
        // 打印空进度条
        printProgressBar(0.0);
        // 在传输进行时更新进度条
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            // Note: bytesTransferred and totalBytesToTransfer aren't used, they're just for documentation purposes.
            TransferProgress progress = transfer.getProgress();
            long bytesTransferred = progress.getBytesTransferred();
            long totalBytesToTransfer = progress.getTotalBytesToTransfer();
            double percentTransferred = progress.getPercentTransferred();
            eraseProgressBar();
            printProgressBar(percentTransferred);
        } while (transfer.isDone() == false);
        // transfer 的最终状态
        TransferState transferState = transfer.getState();
        System.out.println(": " + transferState);
    }

    // Prints progress of a multiple file upload while waiting for it to finish.
    public static void showMultiUploadProgress(MultipleFileUpload multipleFileUpload) {
        // print the upload's human-readable description
        System.out.println(multipleFileUpload.getDescription());

        Collection<? extends Upload> subTransfers = new ArrayList<Upload>();
        subTransfers = multipleFileUpload.getSubTransfers();

        do {
            System.out.println("\nSubtransfer progress:\n");
            for (Upload u : subTransfers) {
                System.out.println("  " + u.getDescription());
                if (u.isDone()) {
                    TransferState transferState = u.getState();
                    System.out.println("  " + transferState);
                } else {
                    TransferProgress progress = u.getProgress();
                    double pct = progress.getPercentTransferred();
                    printProgressBar(pct);
                    System.out.println();
                }
            }

            // wait a bit before the next update.
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                return;
            }
        } while (multipleFileUpload.isDone() == false);
        // print the final state of the transfer.
        TransferState transferState = multipleFileUpload.getState();
        System.out.println("\nMultipleFileUpload " + transferState);
    }

    // prints a simple text progressbar: [#####     ]
    public static void printProgressBar(double pct) {
        // if bar_size changes, then change erase_bar (in eraseProgressBar) to
        // match.
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";
        int amt_full = (int) (bar_size * (pct / 100.0));
        System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
                empty_bar.substring(0, bar_size - amt_full));
    }

    // erases the progress bar.
    public static void eraseProgressBar() {
        // erase_bar is bar_size (from printProgressBar) + 4 chars.
        final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
        System.out.format(erase_bar);
    }

    public static void uploadFileWithListener(String file_path,
                                              String bucket_name, String key_prefix, boolean pause) {
        System.out.println("file: " + file_path +
                (pause ? " (pause)" : ""));

        String key_name = null;
        if (key_prefix != null) {
            key_name = key_prefix + '/' + file_path;
        } else {
            key_name = file_path;
        }

        File f = new File(file_path);
        TransferManager transferManager = TransferManagerBuilder.standard().build();
        try {
            Upload u = transferManager.upload(bucket_name, key_name, f);
            // print an empty progress bar...
            printProgressBar(0.0);
            u.addProgressListener(new ProgressListener() {
                public void progressChanged(ProgressEvent e) {
                    double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
                    eraseProgressBar();
                    printProgressBar(pct);
                }
            });
            // block with Transfer.waitForCompletion()
            TransferManagerProgress.waitForCompletion(u);
            // print the final state of the transfer.
            TransferState transferState = u.getState();
            System.out.println(": " + transferState);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        transferManager.shutdownNow();
    }

    public static void uploadDirWithSubProgress(String dir_path,
                                                String bucket_name, String key_prefix, boolean recursive,
                                                boolean pause) {
        System.out.println("directory: " + dir_path + (recursive ?
                " (recursive)" : "") + (pause ? " (pause)" : ""));

        TransferManager transferManager = new TransferManager();
        try {
            MultipleFileUpload multipleFileUpload = transferManager.uploadDirectory(
                    bucket_name, key_prefix, new File(dir_path), recursive);
            // loop with Transfer.isDone()
            TransferManagerProgress.showMultiUploadProgress(multipleFileUpload);
            // or block with Transfer.waitForCompletion()
            TransferManagerProgress.waitForCompletion(multipleFileUpload);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        transferManager.shutdownNow();
    }

}
