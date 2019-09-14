Using Airflow
============

This airflow dag consists of three part, 'load staging data' , 'load fact and dimension data'  and 'quality chek'. Before airflow job, we create tables by /airflow/create_tables.py

### For using AWS
Configure the AWS access configuration in 'dwh.cfg' file for create_tables.py

### loading data location
Loading data from AWS s3 bucket, "s3a://udacity-dend/".

### create_tables.py
Before airflow job, we create tables by /airflow/create_tables.py

Airflow Dag
==========

## Consist of dags
* load staging data : stage_events_to_redshift, stage_songs_to_redshift 
* load fact and dimension data : load_songplays_table, load_user_dimension_table , load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table
* quality chek : run_quality_checks

#### Load staging data
Load data from S3 to staging table "staging_events","staging_songs" respectively.
* These dags use stage_redshift.py operator file.

#### Load fact and dimension data
Load data from staging talbe to fact table(songplays) and dimesnion tables(songs, users, aritists, time). 
* These dags use load_fact.py, load_dimension.py operator file.
* Operator file use helper file at /airflow/plugins/helpers

#### Quality check
After loading data, check if data is empty or not and data count 
* This uses data_quality.py operator file.
* Operator file use helper file at /airflow/plugins/helpers

