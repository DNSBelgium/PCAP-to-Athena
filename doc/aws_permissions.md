# AWS IAM Policy

## Glue

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "glue:BatchCreatePartition",
                "glue:CreateTable",
                "glue:GetTables",
                "glue:GetPartitions",
                "glue:UpdateTable",
                "athena:GetQueryExecutions",
                "glue:DeleteTable",
                "athena:ListWorkGroups",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:GetPartition",
                "glue:CreatePartition",
                "athena:RunQuery",
                "glue:DeletePartition",
                "glue:UpdatePartition"
            ],
            "Resource": "*"
        }
    ]
}
```

## S3 & Athena

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "VisualEditor0",
            "Effect": "Allow",
            "Action": [
                "s3:DeleteObjectTagging",
                "athena:StartQueryExecution",
                "athena:GetQueryResultsStream",
                "athena:GetQueryResults",
                "s3:ListBucket",
                "athena:ListQueryExecutions",
                "s3:PutObject",
                "s3:GetObject",
                "athena:GetWorkGroup",
                "athena:CancelQueryExecution",
                "athena:GetQueryExecution",
                "athena:StopQueryExecution",
                "s3:GetObjectTagging",
                "s3:PutObjectTagging",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::my-bucket",
                "arn:aws:s3:::my-bucket/*",
                "arn:aws:athena:eu-west-1:000000000000:workgroup/pcap-to-athena"
            ]
        }
    ]
}```
