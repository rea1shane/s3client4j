# s3client4j

AWS S3 Java 客户端。

## 前置条件

### 凭证设置

官方文档：https://docs.aws.amazon.com/zh_cn/sdk-for-java/v1/developer-guide/credentials.html

测试环境中使用了如下方式：

1. 在 macos 的 `~` 路径下创建文件

    ```
    .aws
    ├── config
    └── credentials
    ```
2. `~/.aws/credentials` 内容：

    ```
    [default]
    aws_access_key_id = your_access_key_id
    aws_secret_access_key = your_secret_access_key
    ```

    替换你自己的 `aws_access_key_id` 和你的 `aws_secret_access_key`。

3. `~/.aws/config` 内容：

    ```
    [default]
    region = your_aws_region
    ```

    替换你 AWS 账号所在的区域（例如，“us-east-1”）。