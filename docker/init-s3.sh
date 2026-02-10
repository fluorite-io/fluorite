#!/bin/bash
# Initialize S3 bucket for Turbine

awslocal s3 mb s3://turbine-batches
echo "Created S3 bucket: turbine-batches"
