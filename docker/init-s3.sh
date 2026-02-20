#!/bin/bash
# Initialize S3 bucket for Flourine

awslocal s3 mb s3://flourine-batches
echo "Created S3 bucket: flourine-batches"
