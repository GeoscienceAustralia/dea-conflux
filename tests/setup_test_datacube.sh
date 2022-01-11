#!/usr/bin/env bash
set -ex
export METADATA_CATALOG=https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/a4f39b485b33608a016032d9987251881fec4b6f/workspaces/sandbox-metadata.yaml
export PRODUCT_CATALOG=https://raw.githubusercontent.com/GeoscienceAustralia/dea-config/87ca056fa62900596cbf05612da9033fc763009c/workspaces/sandbox-products.csv

# Setup datacube
docker-compose exec -T index datacube system init --no-default-types --no-init-users
# Setup metadata types
docker-compose exec -T index datacube metadata add "$METADATA_CATALOG"
# Download the product catalog
docker-compose exec -T index wget "$PRODUCT_CATALOG" -O product_list.csv
# Add products for testing from the product list
docker-compose exec -T index bash -c "tail -n+2 product_list.csv | grep 'wofs_albers\|ls7_fc_albers\|ga_ls_wo_3\|ga_ls_fc_3\|ga_ls7e_ard_3' | awk -F , '{print \$2}' | xargs datacube -v product add"

# Index test data
cat > index_tiles.sh <<EOF
# Index one WOfS and FC tile (Belconnen)
# WOfLs
s3-to-dc 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'wofs_albers'
# C3 ARD
s3-to-dc 's3://dea-public-data/baseline/ga_ls7e_ard_3/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls7e_ard_3'
s3-to-dc 's3://dea-public-data/baseline/ga_ls7e_ard_3/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls7e_ard_3' 
# C3 WO
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
# C3 FC
s3-to-dc 's3://dea-public-data/derivative/ga_ls_fc_3/2-5-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_fc_3'
s3-to-dc 's3://dea-public-data/derivative/ga_ls_fc_3/2-5-0/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_fc_3'
# FC
s3-to-dc 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls7/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'ls7_fc_albers'
EOF

cat index_tiles.sh | docker-compose exec -T index bash
