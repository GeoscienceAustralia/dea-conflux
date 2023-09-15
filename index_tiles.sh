#!/bin/bash

# Index wofs_ls tiles covering the waterbody UID: edumesbb2
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/204/049/2023/01/07/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/048/2023/01/06/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/049/2023/01/06/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/048/2023/01/08/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/049/2023/01/08/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
