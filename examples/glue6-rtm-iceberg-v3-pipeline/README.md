# Real-Time Market Risk Pipeline with AWS Glue 6.0 and Apache Iceberg v3

This repository contains the code artifacts for the AWS Big Data Blog post: **Build a Real-Time Market Risk Pipeline with AWS Glue 6.0, Apache Iceberg v3, and Spark Real-Time Mode**.

## Architecture

![Architecture](architecture.png)

The pipeline processes trade pricing vectors from a bank's front-office systems through three paths:

1. **Real-Time Path (Spark RTM, Scala)** — Reads trades from Amazon MSK, evaluates risk thresholds, writes alerts to a downstream Kafka topic within milliseconds.
2. **Near-Real-Time Path (Micro-batch, PySpark)** — Reads from the same MSK topic and stores the full trade history into an Iceberg v3 table with Variant columns and shredding enabled.
3. **Batch Path (PySpark, Arrow UDFs)** — Reads from the Iceberg v3 table, extracts risk metrics from Variant columns using `variant_get()`, and computes Value at Risk (VaR) using Arrow-native UDFs.

### AWS Glue 6.0 Features Demonstrated

| Feature | Where Used |
|---------|-----------|
| Spark Real-Time Mode (RTM) | RTM alerts job — `Trigger.RealTime()` for millisecond-level alerting |
| Iceberg v3 Variant type | NRT ingestion — `PARSE_JSON()` stores heterogeneous pricing vectors natively |
| Variant shredding | NRT ingestion — auto-infers schema for columnar performance |
| `variant_get()` extraction | Batch VaR — type-safe extraction from nested Variant fields |
| Arrow-native UDFs (`@arrow_udf`) | Batch VaR — vectorized VaR computation (1000 Monte Carlo scenarios) |
| Deletion vectors (merge-on-read) | Batch VaR — fast trade amendments and cancellations |
| Column DEFAULT values | NRT ingestion — auto-populated `region` and `asset_class` fields |

## Prerequisites

- An AWS account with AWS Glue 6.0 enabled
- A VPC with at least 2 subnets in different Availability Zones
- A security group that allows TCP traffic on port 9098 within itself (for MSK IAM auth)
- The VPC's main route table ID (for the S3 gateway endpoint)

## Deploy

### Step 1: Launch the CloudFormation Stack

Deploy the stack using the AWS Console or CLI:

```bash
aws cloudformation create-stack \
  --stack-name glue6-rtm-pipeline \
  --template-body file://template.yaml \
  --parameters \
    ParameterKey=VpcId,ParameterValue=<your-vpc-id> \
    ParameterKey=SubnetIds,ParameterValue="<subnet-1>\,<subnet-2>" \
    ParameterKey=SecurityGroupId,ParameterValue=<your-sg-id> \
    ParameterKey=RouteTableId,ParameterValue=<your-route-table-id> \
  --capabilities CAPABILITY_NAMED_IAM \
  --region <region>
```

The stack creates:
- An Amazon MSK cluster (2 brokers, IAM auth, TLS encryption, no public access)
- An S3 bucket for Iceberg table storage and checkpoints (public access blocked)
- An S3 gateway VPC endpoint for private S3 access
- A Glue network connection for MSK VPC connectivity
- A Glue database (`risk_analytics_<account-id>`)
- An IAM role with least-privilege permissions
- 5 AWS Glue 6.0 jobs (topic creator, producer, RTM alerts, NRT ingestion, batch VaR)

Stack creation takes approximately 15–20 minutes (MSK cluster provisioning).

### Step 2: Create Kafka Topics

After the stack completes, run the topic creator job:

```bash
aws glue start-job-run --job-name glue6-rtm-variant-create-topics-<region> --region <region>
```

### Step 3: Run the Pipeline

1. **Start both consumers** (RTM alerts + NRT ingestion):

```bash
aws glue start-job-run --job-name glue6-rtm-variant-rtm-alerts-<region> --region <region>
aws glue start-job-run --job-name glue6-rtm-variant-nrt-ingestion-<region> --region <region>
```

2. **Wait for both to show RUNNING** in the Glue Console, then **start the producer**:

```bash
aws glue start-job-run --job-name glue6-rtm-variant-producer-<region> --region <region>
```

3. **After the producer finishes (~3.5 minutes)**, run the batch VaR job:

```bash
aws glue start-job-run --job-name glue6-rtm-variant-batch-var-<region> --region <region>
```

## View Results

Check CloudWatch Logs (`/aws-glue/jobs/output`) for each job's run ID to see:
- **RTM alerts:** Sub-second latency table showing trade → alert timing
- **NRT ingestion:** Row counts and sample `variant_get()` queries on the Iceberg v3 table
- **Batch VaR:** Arrow UDF VaR results, risk summary by desk/asset class, deletion vector demo

## Parameters

| Parameter | Description |
|-----------|-------------|
| `VpcId` | VPC for MSK cluster. Must have DNS resolution enabled. |
| `SubnetIds` | At least 2 subnets in different AZs within the VPC. |
| `SecurityGroupId` | Security group allowing TCP 9098 within itself (for MSK IAM). |
| `RouteTableId` | Main route table ID (for S3 gateway endpoint). Find in VPC Console → Route Tables → Main=Yes. |
| `ScriptBucket` | S3 location of the Glue scripts. Default: `aws-bigdata-blog/artifacts/glue6-rtm-variant`. |

## Security

- **No public access** on any resource
- S3 bucket: `PublicAccessBlockConfiguration` enabled (all four flags)
- MSK: IAM authentication only, unauthenticated disabled, TLS enforced
- All Glue jobs run inside the VPC via Glue network connection
- S3 access via gateway endpoint (traffic stays on AWS backbone)
- IAM role follows least-privilege principle

## Clean Up

Delete the CloudFormation stack to remove all resources:

```bash
aws cloudformation delete-stack --stack-name glue6-rtm-pipeline --region <region>
```

**Note:** The S3 bucket has `DeletionPolicy: Retain`. Empty and delete it manually after stack deletion:

```bash
aws s3 rm s3://glue6-rtm-variant-<account-id>-<region> --recursive
aws s3api delete-bucket --bucket glue6-rtm-variant-<account-id>-<region> --region <region>
```

## Cost

Estimated cost while the pipeline is running:

| Resource | Cost |
|----------|------|
| MSK (2x kafka.m5.large) | ~$0.35/hour |
| Glue jobs (G.1X workers) | ~$0.44/DPU-hour |
| S3 storage | Negligible for demo data |

**Important:** Delete the stack when done to avoid ongoing MSK charges.

## Repository Structure

```
├── template.yaml              # CloudFormation template
├── architecture.drawio        # Architecture diagram (draw.io)
├── scripts/
│   ├── create_topics.py       # Creates Kafka topics via AdminClient
│   ├── producer.py            # Generates trade events (equities, FX, rates)
│   ├── rtm_alerts.scala       # Spark RTM — risk scoring → alerts topic
│   ├── nrt_ingestion.py       # Micro-batch → Iceberg v3 (Variant + shredding)
│   └── batch_var.py           # Arrow UDF VaR + variant_get + deletion vectors
└── README.md
```

## License

This library is licensed under the MIT-0 License. See the [LICENSE](../../LICENSE.txt) file.
