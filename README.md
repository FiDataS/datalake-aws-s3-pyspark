# s3-data-lake-with-spark-ETL

### Table of Contents

1. [Introduction](#intro)
2. [Project Details](#details)
3. [File Descriptions](#files)
4. [How To Run](#execution)
5. [Data Model](#model)

## Introduction<a name="intro"></a>

This project is part of the Udacity Nanodegree Data Engineer. This is the final project to the chapter about Data Lakes.
The goal of this project is to build an ETL pipeline to load data from S3, process it into separate tables using Spark (Pyspark) and then writing those tables back into S3. This can either be run locally or on an EMR cluster within AWS.

## Project Details<a name="details"></a>

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.
As their data engineer the project now includes the following:
- Building and ETL pipeline that extracts data from S3
- Processes the data using Spark
- Loads the data back into S3 as a set of dimensional tables

This will allow Sparkifys Analytics Team to continue finding insights in what songs their users are listening to.

## File Descriptions <a name="files"></a>

The following files exist within the project

```
S3-DATA-LAKE-WITH-SPARK-ETL/
│
├── README.md
├── data/ --> smaller example data in zip format, full dataset resides within S3
    ├── log-data/ 
    ├── song_data/ 
├── unzipped_data/ --> smaller example data unzipped from data/ folder to use in the Prototype notebooks
    ├── log-data/ 
    ├── song_data/ 
├── output/ --> output folder for Prototype_LOCAL juypter notebook, which was used to develope the logic and use data from unzipped_data as an output
    ├── artists/ 
    ├── songplays/ 
    ├── songs/ 
    ├── time/
    ├── users/ 
├── output_aws/ --> output folder for Prototype_AWS juypter notebook, which was used to load data from S3 and write it to a local folder. The jupyter notebook was later changed to write the output into a created S3 bucket
    ├── artists/ 
    ├── songplays/ 
    ├── songs/ 
    ├── time/
    ├── users/ 
├── dl.cfg --> config file with all necessary details to run on aws (Access key and secret access key from a created IAM user)
├── etl.py --> script reads data from S3, transforms them to five tables, and writes them to S3
├── Prototype_AWS.ipynb --> jupyter notebook for prototyping the code in etl.py - reads and writes to S3
├── Prototype_LOCAL.ipynb --> jupyter notebook for prototyping the code in etl.py - reads and writes from and to local folder
├── LICENSE
```
## How To Run<a name="execution"></a>

- Step 1: Create a dl.cfg file with the following structure containing your AWS credentials (IAM Role with neccessary policies):
```
[AWS]
AWS_ACCESS_KEY_ID='<your access key>'
AWS_SECRET_ACCESS_KEY='<your secret key>'
```
- STEP 3: Run the etl.py file in the terminal. ("python etl.py")

## Data Model<a name="model"></a>

- Fact Table
  - songplays - records in log data associated with song plays i.e. records with page NextSong
    - songplay_id, 
    - start_time, 
    - user_id, level, 
    - song_id, 
    - artist_id, 
    - session_id, 
    - location, 
    - user_agent
  
- Dimension Tables
  - users - users in the app
    - user_id, 
    - first_name, 
    - last_name, 
    - gender, level
  - songs - songs in music database
    - song_id, 
    - title, 
    - artist_id, 
    - year, 
    - duration 
  - artists - artists in music database
    - artist_id, 
    - name, 
    - location, 
    - lattitude, 
    - longitude
  - time - timestamps of records in songplays broken down into specific units
    - start_time, 
    - hour, 
    - day, 
    - week, 
    - month, 
    - year, 
    - weekday