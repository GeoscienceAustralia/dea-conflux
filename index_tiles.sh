# Index one WOfS and FC tile (Belconnen)
# WOfLs
# s3-to-dc 's3://dea-public-data/WOfS/WOFLs/v2.1.5/combined/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'wofs_albers'
# C3 ARD
s3-to-dc 's3://dea-public-data/baseline/ga_ls7e_ard_3/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls7e_ard_3'
s3-to-dc 's3://dea-public-data/baseline/ga_ls7e_ard_3/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls7e_ard_3' 
# C3 WO
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/091/085/2000/02/09/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
s3-to-dc 's3://dea-public-data/derivative/ga_ls_wo_3/1-6-0/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_wo_3'
# C3 FC
s3-to-dc 's3://dea-public-data/derivative/ga_ls_fc_3/2-5-0/090/084/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_fc_3'
s3-to-dc 's3://dea-public-data/derivative/ga_ls_fc_3/2-5-0/090/085/2000/02/02/*.json' --stac --no-sign-request --skip-lineage 'ga_ls_fc_3'
# FC
# s3-to-dc 's3://dea-public-data/fractional-cover/fc/v2.2.1/ls7/x_15/y_-40/2000/02/**/*.yaml' --no-sign-request --skip-lineage 'ls7_fc_albers'
