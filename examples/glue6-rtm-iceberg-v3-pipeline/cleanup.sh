#!/bin/bash
set -e

###############################################################################
# Cleanup — Tears down all resources created by deploy.sh
# Usage: ./cleanup.sh <region>
###############################################################################

REGION="${1:?Usage: ./cleanup.sh <region>}"
STATE_FILE="$(dirname "$0")/.deploy-state-${REGION}"

if [ ! -f "$STATE_FILE" ]; then
    echo "❌ No state file found: $STATE_FILE"
    echo "   Run deploy.sh first, or manually delete resources."
    exit 1
fi

source "$STATE_FILE"

echo "=============================================="
echo " Cleaning up Glue 6.0 RTM Pipeline"
echo " Account: $ACCOUNT | Region: $REGION"
echo "=============================================="

# Phase 1: Stop running Glue jobs
echo ""
echo "=== Stopping Glue jobs ==="
for job in "${PREFIX}-create-topics-${REGION}" "${PREFIX}-producer-${REGION}" "${PREFIX}-rtm-alerts-${REGION}" "${PREFIX}-nrt-ingestion-${REGION}" "${PREFIX}-batch-var-${REGION}"; do
    # Stop any running job runs
    RUNNING=$(aws glue get-job-runs --job-name "$job" --region "$REGION" \
        --query "JobRuns[?JobRunState=='RUNNING'].Id" --output text 2>/dev/null || true)
    for run_id in $RUNNING; do
        aws glue batch-stop-job-run --job-name "$job" --job-run-ids "$run_id" --region "$REGION" 2>/dev/null || true
        echo "  Stopped: $job ($run_id)"
    done
    # Delete the job
    aws glue delete-job --job-name "$job" --region "$REGION" 2>/dev/null || true
    echo "  Deleted job: $job"
done

# Phase 2: Delete Glue connection + database tables + database
echo ""
echo "=== Deleting Glue resources ==="
aws glue delete-connection --connection-name "$CONNECTION_NAME" --region "$REGION" 2>/dev/null || true
echo "  Deleted connection: $CONNECTION_NAME"

for table in trade_risk_vectors daily_risk_summary; do
    aws glue delete-table --database-name "$DB_NAME" --name "$table" --region "$REGION" 2>/dev/null || true
done
aws glue delete-database --name "$DB_NAME" --region "$REGION" 2>/dev/null || true
echo "  Deleted database: $DB_NAME"

# Phase 3: Delete MSK cluster
echo ""
echo "=== Deleting MSK cluster ==="
if [ -n "$CLUSTER_ARN" ]; then
    aws kafka delete-cluster --cluster-arn "$CLUSTER_ARN" --region "$REGION" 2>/dev/null || true
    echo "  Deleting MSK: $CLUSTER_ARN"
    echo "  Waiting for MSK deletion (~5 min)..."
    while true; do
        STATE=$(aws kafka list-clusters --cluster-name-filter "$MSK_CLUSTER_NAME" --region "$REGION" \
            --query "ClusterInfoList[0].State" --output text 2>/dev/null)
        [ "$STATE" = "None" ] || [ -z "$STATE" ] && break
        [ "$STATE" = "null" ] && break
        sleep 30
    done
    echo "  ✅ MSK deleted"
fi

# Phase 4: Empty and delete S3 bucket
echo ""
echo "=== Deleting S3 bucket ==="
aws s3 rm "s3://$BUCKET" --recursive --region "$REGION" 2>/dev/null || true
aws s3api delete-bucket --bucket "$BUCKET" --region "$REGION" 2>/dev/null || true
echo "  ✅ Bucket deleted: $BUCKET"

# Phase 5: Delete IAM role
echo ""
echo "=== Deleting IAM role ==="
aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "BlogPipelineAccess" 2>/dev/null || true
aws iam detach-role-policy --role-name "$ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" 2>/dev/null || true
aws iam delete-role --role-name "$ROLE_NAME" 2>/dev/null || true
echo "  ✅ Role deleted: $ROLE_NAME"

# Phase 6: Delete VPC resources (order matters)
echo ""
echo "=== Deleting VPC ==="

# Delete VPC endpoint
aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$S3_EP" --region "$REGION" 2>/dev/null || true
echo "  Deleted S3 endpoint: $S3_EP"

# Wait for any ENIs to be cleaned up (MSK/Glue leave them behind)
echo "  Waiting for ENIs to detach..."
sleep 30

# Delete leftover ENIs in our subnets
for subnet in $SUBNET1 $SUBNET2; do
    ENIS=$(aws ec2 describe-network-interfaces --region "$REGION" \
        --filters "Name=subnet-id,Values=$subnet" \
        --query "NetworkInterfaces[].NetworkInterfaceId" --output text 2>/dev/null)
    for eni in $ENIS; do
        ATTACH=$(aws ec2 describe-network-interfaces --network-interface-ids "$eni" --region "$REGION" \
            --query "NetworkInterfaces[0].Attachment.AttachmentId" --output text 2>/dev/null)
        if [ "$ATTACH" != "None" ] && [ -n "$ATTACH" ]; then
            aws ec2 detach-network-interface --attachment-id "$ATTACH" --force --region "$REGION" 2>/dev/null || true
            sleep 5
        fi
        aws ec2 delete-network-interface --network-interface-id "$eni" --region "$REGION" 2>/dev/null || true
        echo "  Deleted ENI: $eni"
    done
done

# Delete subnets
aws ec2 delete-subnet --subnet-id "$SUBNET1" --region "$REGION" 2>/dev/null || true
aws ec2 delete-subnet --subnet-id "$SUBNET2" --region "$REGION" 2>/dev/null || true
echo "  Deleted subnets"

# Delete security group
aws ec2 delete-security-group --group-id "$SG_ID" --region "$REGION" 2>/dev/null || true
echo "  Deleted SG: $SG_ID"

# Delete VPC
aws ec2 delete-vpc --vpc-id "$VPC_ID" --region "$REGION" 2>/dev/null || true
echo "  ✅ VPC deleted: $VPC_ID"

# Remove state file
rm -f "$STATE_FILE"

echo ""
echo "=============================================="
echo " ✅ CLEANUP COMPLETE"
echo "=============================================="
