# Sparkify Analytics 

<b>Sparkify</b> is an international media services provider  
It's primary business is to build an <b>audio streaming</b> platform that provides music, videos and podcasts from record labels and media companies  

## Challenge

Sparkify wants to better serve its users and thus needs to analyze the data collected on songs and user activity on their music streaming app. 
The analytical goal is to understand what songs users are listening to  

## Architecture 

The data is currently stored as JSON files and resides on AWS in a S3 bucket (logs on user activity as well as metadata on the songs). 
This architecture doesn't provide an easy way to directly query the data  

## Analytics goals 

Sparkify wants to leverage its cloud investment and use highly scalable components to support the analytical queries. They chose to use Spark (hosted on Amazon) to process their data and Amazon S3 as intermediate and final data storage solution.  

The main idea is to build a 3-step ETL as follow :
* extract the song & log data directly from <b>S3</b> into staging tables  
* build & populate fact & dimension tables from staging according to the <b>(OLAP-oriented) analytic model</b> they define  
* persist these fact & dimension tables back to <b>S3</b> as <b>parquet files</b>  

## configuration

AWS credentials & S3 bucket for persistence must be adjusted to gain access to your own infrastructure  

## Scripts

Run the following commands in the terminal:  
* `python etl.py` to process the files (logs/songs) stored in a S3 bucket (raw data), populate first the staging tables and then the fact and dimension tables after data validation and transformation, and finally persist the tables in another S3 bucket

At the end of the process the <b>parquet files</b> can be requested for analysis purposes using BI tools.