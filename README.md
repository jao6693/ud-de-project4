![Sparkify](https://github.com/jao6693/ud-de-project4/blob/master/img/sparkify_logo.png?raw=true)

# Sparkify Analytics 

<b>Sparkify</b> is an international media services provider  
It's primary business is to build an <b>audio streaming</b> platform that provides music, videos and podcasts from record labels and media companies  

## Challenge

Sparkify wants to better serve its users and thus needs to analyze the data collected on songs and user activity on their music streaming app. The analytical goal is to understand what songs users are listening to  

## Architecture 

The data is currently stored as JSON files and resides on AWS in a S3 bucket (logs on user activity as well as metadata on the songs). This architecture doesn't provide an easy way to directly query the data  

## Analytics goals 

Sparkify wants to leverage its cloud investment and use highly scalable components to support the analytical queries. They chose to use Amazon EMR (with Spark) to process their data and Amazon S3 as intermediate and final data storage solution.  

The main idea is to build a 3-step ETL process as follow :
* extract the song & log data directly from <b>S3</b> into staging tables  
* build & populate fact & dimension tables from staging according to the <b>(OLAP-oriented) analytic model</b> they define  
* persist these fact & dimension tables back to <b>S3</b> as <b>parquet files</b>  

## Design considerations

The analytical model is designed the following way:  
* one fact table called `songplays` containing facts (like level, location, user_agent, ...)
* four dimension tables called `users`, `songs`, `artists`, `time` contain additional information on songs, artists, users, ...

These tables are connected according to the following schema:  

![analytic model representation](https://github.com/jao6693/ud-de-project4/blob/master/img/analytic_model.png?raw=true)

With this model analysts are able to easily query informations directly from tables or plus BI tools on top of it   

## Files & Configuration

The following files are available within the project
* dl.cfg:  
This file is used to set the configuration of the ETL process  
In section `[AWS]`, set your own credentials in order to access AWS and interact with it
In section `[PROJECT]`, set `MODE` to either `LOCAL` to work on data in your own workspace or `S3` to set input/output folders as `S3` buckets  
* etl.py:
This file is used to retrieve log & song data, process them with Spark, and write output parquet files   

## Scripts

Run the following commands in your terminal:  
* `python etl.py`
It will process the files (logs/songs) stored in a S3 bucket (raw data), populate first the staging tables and then the fact and dimension tables after data validation and transformation, and finally persist the tables in another S3 bucket

At the end of the process the <b>parquet files</b> can be requested for analysis purposes using BI tools