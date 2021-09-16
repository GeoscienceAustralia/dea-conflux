#!/usr/bin/env bash
set -ex

# Setup datacube
docker-compose exec -T index datacube system init --no-default-types --no-init-users
# Setup metadata types
docker-compose exec -T index datacube metadata add "$METADATA_CATALOG"
# Index products we care about for dea-waterbodies
docker-compose exec -T index wget "$PRODUCT_CATALOG" -O product_list.csv
docker-compose exec -T index bash -c "tail -n+2 product_list.csv | grep 'wofs_albers\|ls7_fc_albers\|ga_ls_wo_3' | awk -F , '{print \$2}' | xargs datacube -v product add" 

# Index WOfS and Coastline
cat > index_tiles.sh <<EOF
# Index one WOfS and FC tile (Belconnen)
# WOfLs
s3-to-dc 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'wofs_albers'
# C3 WO
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
# FC
s3-to-dc 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls7/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'ls7_fc_albers'
EOF

cat index_tiles.sh | docker-compose exec -T index bash
