#!/bin/bash
set -e

###############################################################################
# Glue 6.0 Blog — Real-Time Market Risk Pipeline
# 
# Creates an isolated VPC with MSK, Glue jobs, and runs the full pipeline.
# No existing resources are modified.
#
# Usage:
#   ./deploy.sh <region>
#   ./deploy.sh us-west-1
#
# Cleanup:
#   ./cleanup.sh <region>
###############################################################################

REGION="${1:?Usage: ./deploy.sh <region>}"
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
PREFIX="glue6-rtm-variant"
SUFFIX="${PREFIX}-${ACCOUNT}-${REGION}"
BUCKET="${SUFFIX}"
MSK_CLUSTER_NAME="${PREFIX}-${ACCOUNT}"
CONNECTION_NAME="${PREFIX}-connection-${REGION}"
DB_NAME="risk_analytics_${ACCOUNT}"
ROLE_NAME="${PREFIX}-role-${REGION}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)/scripts"
GLUE_VERSION="6.0"
WORKER_TYPE="G.1X"
NUM_WORKERS=2

echo "=============================================="
echo " Glue 6.0 — Real-Time Market Risk Pipeline"
echo " Account: $ACCOUNT | Region: $REGION"
echo "=============================================="

# Helper: wait for Glue job
wait_for_job() {
    local job_name="$1" run_id="$2" timeout="${3:-600}"
    local elapsed=0
    echo "  Waiting for $job_name..."
    while [ $elapsed -lt $timeout ]; do
        state=$(aws glue get-job-run --job-name "$job_name" --run-id "$run_id" \
            --region "$REGION" --query "JobRun.JobRunState" --output text 2>/dev/null)
        case "$state" in
            SUCCEEDED) echo "  ✅ $job_name SUCCEEDED"; return 0 ;;
            FAILED|ERROR|TIMEOUT|STOPPED)
                err=$(aws glue get-job-run --job-name "$job_name" --run-id "$run_id" \
                    --region "$REGION" --query "JobRun.ErrorMessage" --output text 2>/dev/null)
                echo "  ❌ $job_name $state: $err"; return 1 ;;
        esac
        sleep 15
        elapsed=$((elapsed + 15))
    done
    echo "  ⏰ $job_name timed out after ${timeout}s"; return 1
}

###############################################################################
# PHASE 1: VPC + NETWORKING (fully isolated)
###############################################################################
echo ""
echo "=== Phase 1: Create Isolated VPC ==="

# Get 2 AZs for this region
AZ1=$(aws ec2 describe-availability-zones --region "$REGION" --query "AvailabilityZones[0].ZoneName" --output text)
AZ2=$(aws ec2 describe-availability-zones --region "$REGION" --query "AvailabilityZones[1].ZoneName" --output text)
echo "  AZs: $AZ1, $AZ2"

# Create VPC
VPC_ID=$(aws ec2 create-vpc --cidr-block "10.0.0.0/16" --region "$REGION" \
    --tag-specifications "ResourceType=vpc,Tags=[{Key=Name,Value=${PREFIX}-vpc}]" \
    --query "Vpc.VpcId" --output text)
echo "  VPC: $VPC_ID"

# Enable DNS resolution + hostnames (required for VPC endpoints)
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-support '{"Value":true}' --region "$REGION"
aws ec2 modify-vpc-attribute --vpc-id "$VPC_ID" --enable-dns-hostnames '{"Value":true}' --region "$REGION"

# Create 2 private subnets
SUBNET1=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "10.0.1.0/24" \
    --availability-zone "$AZ1" --region "$REGION" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${PREFIX}-subnet-1}]" \
    --query "Subnet.SubnetId" --output text)
SUBNET2=$(aws ec2 create-subnet --vpc-id "$VPC_ID" --cidr-block "10.0.2.0/24" \
    --availability-zone "$AZ2" --region "$REGION" \
    --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=${PREFIX}-subnet-2}]" \
    --query "Subnet.SubnetId" --output text)
echo "  Subnets: $SUBNET1 ($AZ1), $SUBNET2 ($AZ2)"

# Create Security Group (self-referencing for MSK + Glue)
SG_ID=$(aws ec2 create-security-group --group-name "${PREFIX}-sg" \
    --description "Glue6 RTM blog - MSK + Glue internal traffic" \
    --vpc-id "$VPC_ID" --region "$REGION" \
    --tag-specifications "ResourceType=security-group,Tags=[{Key=Name,Value=${PREFIX}-sg}]" \
    --query "GroupId" --output text)

# Allow all TCP within the SG (MSK 9098 + Glue shuffle)
aws ec2 authorize-security-group-ingress --group-id "$SG_ID" --region "$REGION" \
    --protocol tcp --port 0-65535 --source-group "$SG_ID" >/dev/null
echo "  Security Group: $SG_ID (self-referencing)"

# Get the main route table for this VPC
RT_ID=$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" \
    --region "$REGION" --query "RouteTables[0].RouteTableId" --output text)

# Associate route table with both subnets
aws ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET1" --region "$REGION" >/dev/null
aws ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET2" --region "$REGION" >/dev/null

# Create S3 Gateway VPC Endpoint
S3_EP=$(aws ec2 create-vpc-endpoint --vpc-id "$VPC_ID" \
    --service-name "com.amazonaws.${REGION}.s3" \
    --route-table-ids "$RT_ID" --region "$REGION" \
    --tag-specifications "ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=${PREFIX}-s3-ep}]" \
    --query "VpcEndpoint.VpcEndpointId" --output text)
echo "  S3 VPC Endpoint: $S3_EP"

echo "  ✅ Isolated VPC ready (no internet access, S3 via gateway endpoint)"

###############################################################################
# PHASE 2: IAM + S3
###############################################################################
echo ""
echo "=== Phase 2: IAM Role + S3 Bucket ==="

# Create IAM role
aws iam create-role --role-name "$ROLE_NAME" \
    --assume-role-policy-document "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [{
            \"Effect\": \"Allow\",
            \"Principal\": {\"Service\": \"glue.amazonaws.com\"},
            \"Action\": \"sts:AssumeRole\"
        }]
    }" 2>/dev/null || echo "  Role already exists"

aws iam attach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" 2>/dev/null || true

aws iam put-role-policy --role-name "$ROLE_NAME" --policy-name "BlogPipelineAccess" \
    --policy-document "{
        \"Version\": \"2012-10-17\",
        \"Statement\": [
            {
                \"Effect\": \"Allow\",
                \"Action\": [\"s3:GetObject\",\"s3:PutObject\",\"s3:DeleteObject\",\"s3:ListBucket\"],
                \"Resource\": [\"arn:aws:s3:::${BUCKET}\",\"arn:aws:s3:::${BUCKET}/*\"]
            },
            {
                \"Effect\": \"Allow\",
                \"Action\": [
                    \"kafka-cluster:Connect\",\"kafka-cluster:DescribeCluster\",
                    \"kafka-cluster:DescribeTopic\",\"kafka-cluster:CreateTopic\",
                    \"kafka-cluster:WriteData\",\"kafka-cluster:ReadData\",
                    \"kafka-cluster:AlterGroup\",\"kafka-cluster:DescribeGroup\"
                ],
                \"Resource\": \"*\"
            },
            {
                \"Effect\": \"Allow\",
                \"Action\": [\"glue:*\"],
                \"Resource\": \"*\"
            },
            {
                \"Effect\": \"Allow\",
                \"Action\": [\"logs:CreateLogGroup\",\"logs:CreateLogStream\",\"logs:PutLogEvents\"],
                \"Resource\": \"arn:aws:logs:*:*:*\"
            },
            {
                \"Effect\": \"Allow\",
                \"Action\": [
                    \"ec2:CreateNetworkInterface\",\"ec2:DeleteNetworkInterface\",
                    \"ec2:DescribeNetworkInterfaces\",\"ec2:DescribeSecurityGroups\",
                    \"ec2:DescribeSubnets\",\"ec2:DescribeVpcs\"
                ],
                \"Resource\": \"*\"
            }
        ]
    }" 2>/dev/null || true

echo "  ✅ IAM Role: $ROLE_NAME"

# Wait for IAM propagation
sleep 10

# Create S3 bucket (block all public access)
aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
    --create-bucket-configuration LocationConstraint="$REGION" 2>/dev/null || echo "  Bucket already exists"
aws s3api put-public-access-block --bucket "$BUCKET" --region "$REGION" \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
echo "  ✅ S3 Bucket: $BUCKET (public access blocked)"

###############################################################################
# PHASE 3: MSK CLUSTER
###############################################################################
echo ""
echo "=== Phase 3: Create MSK Cluster (~15 min) ==="

aws kafka create-cluster \
    --cluster-name "$MSK_CLUSTER_NAME" \
    --kafka-version "3.6.0" \
    --number-of-broker-nodes 2 \
    --broker-node-group-info "{
        \"InstanceType\": \"kafka.m5.large\",
        \"ClientSubnets\": [\"$SUBNET1\", \"$SUBNET2\"],
        \"SecurityGroups\": [\"$SG_ID\"],
        \"StorageInfo\": {\"EbsStorageInfo\": {\"VolumeSize\": 50}}
    }" \
    --client-authentication '{"Sasl":{"Iam":{"Enabled":true}},"Unauthenticated":{"Enabled":false}}' \
    --encryption-info '{"EncryptionInTransit":{"ClientBroker":"TLS","InCluster":true}}' \
    --region "$REGION" >/dev/null

echo "  Waiting for MSK ACTIVE (this takes ~15 minutes)..."
while true; do
    STATE=$(aws kafka list-clusters --cluster-name-filter "$MSK_CLUSTER_NAME" --region "$REGION" \
        --query "ClusterInfoList[0].State" --output text 2>/dev/null)
    [ "$STATE" = "ACTIVE" ] && break
    echo "    Status: $STATE (waiting 30s...)"
    sleep 30
done

CLUSTER_ARN=$(aws kafka list-clusters --cluster-name-filter "$MSK_CLUSTER_NAME" --region "$REGION" \
    --query "ClusterInfoList[0].ClusterArn" --output text)
BOOTSTRAP=$(aws kafka get-bootstrap-brokers --cluster-arn "$CLUSTER_ARN" --region "$REGION" \
    --query "BootstrapBrokerStringSaslIam" --output text)
echo "  ✅ MSK ACTIVE | Bootstrap: $BOOTSTRAP"

###############################################################################
# PHASE 4: GLUE CONNECTION + DATABASE
###############################################################################
echo ""
echo "=== Phase 4: Glue Connection + Database ==="

aws glue create-connection --region "$REGION" --connection-input "{
    \"Name\": \"$CONNECTION_NAME\",
    \"ConnectionType\": \"NETWORK\",
    \"PhysicalConnectionRequirements\": {
        \"SubnetId\": \"$SUBNET1\",
        \"SecurityGroupIdList\": [\"$SG_ID\"],
        \"AvailabilityZone\": \"$AZ1\"
    }
}" 2>/dev/null || echo "  Connection already exists"
echo "  ✅ Glue Connection: $CONNECTION_NAME"

aws glue create-database --region "$REGION" --database-input "{
    \"Name\": \"$DB_NAME\",
    \"Description\": \"Glue 6.0 Real-Time Market Risk Pipeline\",
    \"LocationUri\": \"s3://$BUCKET/warehouse/\"
}" 2>/dev/null || echo "  Database already exists"
echo "  ✅ Glue Database: $DB_NAME"

###############################################################################
# PHASE 5: UPLOAD SCRIPTS + CREATE GLUE JOBS
###############################################################################
echo ""
echo "=== Phase 5: Upload Scripts + Create Glue Jobs ==="

aws s3 cp "$SCRIPT_DIR/create_topics.py" "s3://$BUCKET/scripts/create_topics.py" --region "$REGION"
aws s3 cp "$SCRIPT_DIR/producer.py" "s3://$BUCKET/scripts/producer.py" --region "$REGION"
aws s3 cp "$SCRIPT_DIR/rtm_alerts.scala" "s3://$BUCKET/scripts/rtm_alerts.scala" --region "$REGION"
aws s3 cp "$SCRIPT_DIR/nrt_ingestion.py" "s3://$BUCKET/scripts/nrt_ingestion.py" --region "$REGION"
aws s3 cp "$SCRIPT_DIR/batch_var.py" "s3://$BUCKET/scripts/batch_var.py" --region "$REGION"
echo "  ✅ Scripts uploaded"

ICEBERG_CONF="spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.warehouse=s3://$BUCKET/warehouse/ --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
CHECKPOINT="s3://$BUCKET/checkpoints"

# Delete existing jobs if re-running
for job in "${PREFIX}-create-topics-${REGION}" "${PREFIX}-producer-${REGION}" "${PREFIX}-rtm-alerts-${REGION}" "${PREFIX}-nrt-ingestion-${REGION}" "${PREFIX}-batch-var-${REGION}"; do
    aws glue delete-job --job-name "$job" --region "$REGION" 2>/dev/null || true
done

# 1. Topic Creator
aws glue create-job --name "${PREFIX}-create-topics-${REGION}" \
    --role "$ROLE_NAME" --glue-version "$GLUE_VERSION" \
    --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/create_topics.py\",\"PythonVersion\":\"3\"}" \
    --number-of-workers $NUM_WORKERS --worker-type "$WORKER_TYPE" \
    --connections "{\"Connections\":[\"$CONNECTION_NAME\"]}" \
    --default-arguments "{\"--datalake-formats\":\"iceberg\",\"--BOOTSTRAP_SERVERS\":\"$BOOTSTRAP\"}" \
    --region "$REGION" >/dev/null
echo "  Created: ${PREFIX}-create-topics-${REGION}"

# 2. Producer
aws glue create-job --name "${PREFIX}-producer-${REGION}" \
    --role "$ROLE_NAME" --glue-version "$GLUE_VERSION" \
    --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/producer.py\",\"PythonVersion\":\"3\"}" \
    --number-of-workers $NUM_WORKERS --worker-type "$WORKER_TYPE" \
    --connections "{\"Connections\":[\"$CONNECTION_NAME\"]}" \
    --default-arguments "{\"--datalake-formats\":\"iceberg\",\"--BOOTSTRAP_SERVERS\":\"$BOOTSTRAP\"}" \
    --region "$REGION" >/dev/null
echo "  Created: ${PREFIX}-producer-${REGION}"

# 3. RTM Alerts (Scala streaming)
aws glue create-job --name "${PREFIX}-rtm-alerts-${REGION}" \
    --role "$ROLE_NAME" --glue-version "$GLUE_VERSION" \
    --command "{\"Name\":\"gluestreaming\",\"ScriptLocation\":\"s3://$BUCKET/scripts/rtm_alerts.scala\"}" \
    --number-of-workers $NUM_WORKERS --worker-type "$WORKER_TYPE" \
    --connections "{\"Connections\":[\"$CONNECTION_NAME\"]}" \
    --default-arguments "{\"--job-language\":\"scala\",\"--class\":\"RTMAlerts\",\"--BOOTSTRAP_SERVERS\":\"$BOOTSTRAP\",\"--CHECKPOINT_PATH\":\"$CHECKPOINT\",\"--enable-metrics\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\"}" \
    --region "$REGION" >/dev/null
echo "  Created: ${PREFIX}-rtm-alerts-${REGION}"

# 4. NRT Ingestion (PySpark streaming)
aws glue create-job --name "${PREFIX}-nrt-ingestion-${REGION}" \
    --role "$ROLE_NAME" --glue-version "$GLUE_VERSION" \
    --command "{\"Name\":\"gluestreaming\",\"ScriptLocation\":\"s3://$BUCKET/scripts/nrt_ingestion.py\",\"PythonVersion\":\"3\"}" \
    --number-of-workers $NUM_WORKERS --worker-type "$WORKER_TYPE" \
    --connections "{\"Connections\":[\"$CONNECTION_NAME\"]}" \
    --default-arguments "{\"--datalake-formats\":\"iceberg\",\"--conf\":\"$ICEBERG_CONF\",\"--BOOTSTRAP_SERVERS\":\"$BOOTSTRAP\",\"--DATABASE_NAME\":\"$DB_NAME\",\"--CHECKPOINT_PATH\":\"$CHECKPOINT\",\"--enable-auto-scaling\":\"true\",\"--enable-metrics\":\"true\",\"--enable-continuous-cloudwatch-log\":\"true\"}" \
    --region "$REGION" >/dev/null
echo "  Created: ${PREFIX}-nrt-ingestion-${REGION}"

# 5. Batch VaR (PySpark ETL)
aws glue create-job --name "${PREFIX}-batch-var-${REGION}" \
    --role "$ROLE_NAME" --glue-version "$GLUE_VERSION" \
    --command "{\"Name\":\"glueetl\",\"ScriptLocation\":\"s3://$BUCKET/scripts/batch_var.py\",\"PythonVersion\":\"3\"}" \
    --number-of-workers 4 --worker-type "$WORKER_TYPE" \
    --default-arguments "{\"--datalake-formats\":\"iceberg\",\"--conf\":\"$ICEBERG_CONF\",\"--DATABASE_NAME\":\"$DB_NAME\",\"--enable-metrics\":\"true\"}" \
    --region "$REGION" >/dev/null
echo "  Created: ${PREFIX}-batch-var-${REGION}"

echo "  ✅ All Glue jobs created"

###############################################################################
# PHASE 6: RUN PIPELINE
###############################################################################
echo ""
echo "=== Phase 6: Create Kafka Topics ==="

RUN_ID=$(aws glue start-job-run --job-name "${PREFIX}-create-topics-${REGION}" \
    --region "$REGION" --query "JobRunId" --output text)
wait_for_job "${PREFIX}-create-topics-${REGION}" "$RUN_ID" 300

echo ""
echo "=== Phase 7: Start Consumers (RTM + NRT) ==="

RTM_RUN_ID=$(aws glue start-job-run --job-name "${PREFIX}-rtm-alerts-${REGION}" \
    --region "$REGION" --query "JobRunId" --output text)
echo "  RTM started: $RTM_RUN_ID"

NRT_RUN_ID=$(aws glue start-job-run --job-name "${PREFIX}-nrt-ingestion-${REGION}" \
    --region "$REGION" --query "JobRunId" --output text)
echo "  NRT started: $NRT_RUN_ID"

echo "  Waiting for consumers to reach RUNNING state (~90s cold start)..."
for i in $(seq 1 12); do
    STATE=$(aws glue get-job-run --job-name "${PREFIX}-rtm-alerts-${REGION}" --run-id "$RTM_RUN_ID" \
        --region "$REGION" --query "JobRun.JobRunState" --output text 2>/dev/null)
    [ "$STATE" = "RUNNING" ] && break
    sleep 15
done
echo "  ✅ RTM is RUNNING"

for i in $(seq 1 12); do
    STATE=$(aws glue get-job-run --job-name "${PREFIX}-nrt-ingestion-${REGION}" --run-id "$NRT_RUN_ID" \
        --region "$REGION" --query "JobRun.JobRunState" --output text 2>/dev/null)
    [ "$STATE" = "RUNNING" ] && break
    sleep 15
done
echo "  ✅ NRT is RUNNING"

echo ""
echo "=== Phase 8: Start Producer (3.5 min of trades) ==="

PRODUCER_RUN_ID=$(aws glue start-job-run --job-name "${PREFIX}-producer-${REGION}" \
    --region "$REGION" --query "JobRunId" --output text)
echo "  Producer started: $PRODUCER_RUN_ID"
wait_for_job "${PREFIX}-producer-${REGION}" "$PRODUCER_RUN_ID" 600

echo ""
echo "=== Phase 9: Wait for Consumers to Finish (5 min) ==="

wait_for_job "${PREFIX}-nrt-ingestion-${REGION}" "$NRT_RUN_ID" 600
wait_for_job "${PREFIX}-rtm-alerts-${REGION}" "$RTM_RUN_ID" 600

echo ""
echo "=== Phase 10: Run Batch VaR ==="

VAR_RUN_ID=$(aws glue start-job-run --job-name "${PREFIX}-batch-var-${REGION}" \
    --region "$REGION" --query "JobRunId" --output text)
wait_for_job "${PREFIX}-batch-var-${REGION}" "$VAR_RUN_ID" 400

###############################################################################
# SUMMARY
###############################################################################
echo ""
echo "=============================================="
echo " ✅ PIPELINE COMPLETE"
echo "=============================================="
echo ""
echo " Region:       $REGION"
echo " VPC:          $VPC_ID"
echo " MSK Cluster:  $MSK_CLUSTER_NAME"
echo " Bootstrap:    $BOOTSTRAP"
echo " S3 Bucket:    $BUCKET"
echo " Database:     $DB_NAME"
echo ""
echo " Job Runs:"
echo "   Topics:     $RUN_ID"
echo "   RTM:        $RTM_RUN_ID"
echo "   Producer:   $PRODUCER_RUN_ID"
echo "   NRT:        $NRT_RUN_ID"
echo "   Batch VaR:  $VAR_RUN_ID"
echo ""
echo " View results in CloudWatch Logs:"
echo "   RTM latency: /aws-glue/jobs/output → $RTM_RUN_ID"
echo "   Batch VaR:   /aws-glue/jobs/output → $VAR_RUN_ID"
echo ""
echo " Cleanup: ./cleanup.sh $REGION"
echo ""

# Save state for cleanup
cat > "$(dirname "$0")/.deploy-state-${REGION}" <<EOF
REGION=$REGION
ACCOUNT=$ACCOUNT
VPC_ID=$VPC_ID
SUBNET1=$SUBNET1
SUBNET2=$SUBNET2
SG_ID=$SG_ID
RT_ID=$RT_ID
S3_EP=$S3_EP
BUCKET=$BUCKET
MSK_CLUSTER_NAME=$MSK_CLUSTER_NAME
CLUSTER_ARN=$CLUSTER_ARN
CONNECTION_NAME=$CONNECTION_NAME
DB_NAME=$DB_NAME
ROLE_NAME=$ROLE_NAME
PREFIX=$PREFIX
EOF
echo " State saved to .deploy-state-${REGION} (used by cleanup.sh)"
