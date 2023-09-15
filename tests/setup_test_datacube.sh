#!/usr/bin/env bash
set -ex
export PRODUCT_CATALOG=https://raw.githubusercontent.com/digitalearthafrica/config/master/prod/products_prod.csv

# Setup datacube
docker-compose exec -T index datacube -v system init --no-default-types --no-init-users
# Download the product catalog
docker-compose exec -T index wget "$PRODUCT_CATALOG" -O product_list.csv
# Add products for testing from the product list.
docker-compose exec -T index bash -c "tail -n+2 product_list.csv | grep 'wofs_ls' | awk -F , '{print \$2}' | xargs datacube -v product add"

# Index test data
cat > index_tiles.sh <<EOF
# Index tiles covering the waterbody UID: edumesbb2
# wofs_ls
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/204/049/2023/01/07/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/048/2023/01/06/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/205/049/2023/01/06/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/048/2023/01/08/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
s3-to-dc "s3://deafrica-services/wofs_ls/1-0-0/206/049/2023/01/08/*.json" --stac --no-sign-request --skip-lineage 'wofs_ls'
EOF

cat index_tiles.sh | docker-compose exec -T index bash
