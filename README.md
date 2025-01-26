## 📦 ffbox

Work in progress.

**A better docker for deploying LLM or large AI model apps.** With our container fast streaming technology, you get instant inference, nearly 0 cold start time for your deployed LLM app

Deploy your python AI app to cloud by just pushing your code to s3, and run it with a single command.

### Usage

Push local python project to s3 bucket

`cd my_python_project`

`ffbox push "s3://my_bucket/my_python_project"`

Pull s3 bucket to local directory

`ffbox pull "s3://my-bucket/flux_image_gen"`

Run python project

`ffbox run "s3://my-bucket/flux_image_gen"`

### Benchmarks

no cache in ffbox_cache/ cold start - Pillow image processing:

`ffbox run "s3://ff-image-gen/sd3"` - 11s

### Example S3 bucket public read policy

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": "*",
            "Action": [
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:ListBucket",
                "s3:ListBucketVersions",
                "s3:GetBucketLocation",
                "s3:GetObjectAcl",
                "s3:GetBucketAcl",
                "s3:GetObjectTagging",
                "s3:GetBucketTagging",
                "s3:GetBucketPolicy",
                "s3:GetBucketCORS",
                "s3:GetBucketLogging",
                "s3:GetBucketNotification",
                "s3:GetBucketPolicyStatus",
                "s3:GetBucketRequestPayment",
                "s3:GetBucketVersioning",
                "s3:GetBucketWebsite",
                "s3:GetLifecycleConfiguration",
                "s3:GetReplicationConfiguration",
                "s3:GetAccelerateConfiguration",
                "s3:GetEncryptionConfiguration",
                "s3:GetBucketOwnershipControls"
            ],
            "Resource": [
                "arn:aws:s3:::ff-image-gen",
                "arn:aws:s3:::ff-image-gen/*"
            ]
        }
    ]
}

```
