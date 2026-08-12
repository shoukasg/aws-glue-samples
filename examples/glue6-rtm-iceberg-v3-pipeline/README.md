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
- AWS CLI v2 configured with credentials that have permissions to create:
  - VPC, Subnets, Security Groups, VPC Endpoints
  - Amazon MSK clusters
  - IAM Roles and Policies
  - AWS Glue databases, connections, and jobs
  - S3 buckets
- Bash shell (macOS, Linux, or WSL on Windows)

## Deploy

```bash
git clone https://github.com/aws-samples/sample-glue6-rtm-iceberg-v3-pipeline.git
cd sample-glue6-rtm-iceberg-v3-pipeline
./deploy.sh <region>
```

Example:
```bash
./deploy.sh us-west-1
```

The script takes approximately 20–25 minutes to complete:
- ~15 minutes for MSK cluster provisioning
- ~5 minutes for Glue job cold starts
- ~3.5 minutes for the trade producer to generate events

### What `deploy.sh` Creates

All resources are created in an **isolated VPC** — no existing infrastructure is modified.

| Resource | Details |
|----------|---------|
| VPC | `10.0.0.0/16` with DNS enabled, no internet gateway |
| 2 Private Subnets | In different AZs for MSK HA |
| Security Group | Self-referencing (internal traffic only) |
| S3 Gateway Endpoint | Private path to S3 (no internet needed) |
| S3 Bucket | Iceberg warehouse + scripts + checkpoints (public access blocked) |
| Amazon MSK | 2-broker cluster, IAM auth, TLS encryption, no public access |
| IAM Role | Least-privilege for Glue jobs |
| Glue Connection | Network connection into the VPC |
| Glue Database | `risk_analytics_<account_id>` |
| 5 Glue 6.0 Jobs | Topic creator, producer, RTM alerts, NRT ingestion, batch VaR |

### Pipeline Execution Order

The deploy script runs the jobs in the correct sequence:

1. **Create topics** — Creates `trade-risk-vectors` and `trade-alerts` Kafka topics
2. **Start consumers** — RTM alerts + NRT ingestion (started in parallel, wait for RUNNING)
3. **Start producer** — Sends ~840 trades over 3.5 minutes (equities, FX, rates)
4. **Run batch VaR** — After producer finishes, computes VaR from Iceberg v3 table

## View Results

After deployment completes, view results in CloudWatch Logs:

```bash
# RTM alert latency (should show sub-second processing)
aws logs get-log-events --log-group-name /aws-glue/jobs/output \
  --log-stream-name <RTM_RUN_ID> --region <region> \
  --query "events[].message" --output text

# Batch VaR results (variant_get + Arrow UDF output)
aws logs get-log-events --log-group-name /aws-glue/jobs/output \
  --log-stream-name <VAR_RUN_ID> --region <region> \
  --query "events[].message" --output text
```

The deploy script prints the run IDs at completion.

## Clean Up

```bash
./cleanup.sh <region>
```

This removes **all** resources created by `deploy.sh`, including the VPC, MSK cluster, S3 bucket (emptied first), IAM role, Glue jobs, connection, and database.

## Repository Structure

```
├── deploy.sh                  # One-command deployment
├── cleanup.sh                 # One-command teardown
├── architecture.drawio        # Architecture diagram (draw.io)
├── scripts/
│   ├── create_topics.py       # Creates Kafka topics via AdminClient
│   ├── producer.py            # Generates trade events (equities, FX, rates)
│   ├── rtm_alerts.scala       # Spark RTM — risk scoring → alerts topic
│   ├── nrt_ingestion.py       # Micro-batch → Iceberg v3 (Variant + shredding)
│   └── batch_var.py           # Arrow UDF VaR + variant_get + deletion vectors
├── LICENSE
├── CODE_OF_CONDUCT.md
├── CONTRIBUTING.md
└── README.md
```

## Security

- **No public access** on any resource (S3 public access blocked, MSK IAM-only with TLS, no internet gateway)
- All networking is VPC-internal via private subnets and S3 gateway endpoint
- IAM role follows least-privilege principle
- See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for reporting security issues

## Cost

Estimated cost while the pipeline is running:

| Resource | Cost |
|----------|------|
| MSK (2x kafka.m5.large) | ~$0.35/hour |
| Glue jobs (G.1X workers) | ~$0.44/DPU-hour |
| S3 storage | Negligible for demo data |

**Important:** Run `./cleanup.sh` when done to avoid ongoing MSK charges.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.
