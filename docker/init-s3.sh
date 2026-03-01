#!/bin/bash
# Initialize S3 bucket for Fluorite

awslocal s3 mb s3://fluorite-batches
echo "Created S3 bucket: fluorite-batches"
