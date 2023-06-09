# Youtube-Data-Analysis
----------------------------------------------------------------
# Overview
----------------------------------------------------------------
This project aims to securely manage, streamline, and perform analysis on the structured and semi-structured YouTube videos data based on the video categories and the trending metrics.
----------------------------------------------------------------
# Project Goals
----------------------------------------------------------------
1.Data Ingestion — Build a mechanism to ingest data from different sources
2.ETL System — We are getting data in raw format, transforming this data into the proper format
3.Data lake — We will be getting data from multiple sources so we need centralized repo to store them
4.Scalability — As the size of our data increases, we need to make sure our system scales with it
5.Cloud — We can’t process vast amounts of data on our local computer so we need to use the cloud, in this case, we will use AWS
6.Reporting — Build a dashboard to get answers to the question we asked earlier

----------------------------------------------------------------
# Services we will be using
----------------------------------------------------------------

1. Amazon S3: Amazon S3 is an object storage service that provides manufacturing scalability, data availability, security, and performance.
2. AWS IAM: This is nothing but identity and access management which enables us to manage access to AWS services and resources securely.
3. QuickSight: Amazon QuickSight is a scalable, serverless, embeddable, machine learning-powered business intelligence (BI) service built for the cloud.
4. AWS Glue: A serverless data integration service that makes it easy to discover, prepare, and combine data for analytics, machine learning, and application development.
5. AWS Lambda: Lambda is a computing service that allows programmers to run code without creating or managing servers.
6. AWS Athena: Athena is an interactive query service for S3 in which there is no need to load data it stays in S3.
----------------------------------------------------------------
 # Dataset Used
 ----------------------------------------------------------------
This Kaggle dataset contains statistics (CSV files) on daily popular YouTube videos over the course of many months. There are up to 200 trending videos published every day for many locations. The data for each region is in its own file. The video title, channel title, publication time, tags, views, likes and dislikes, description, and comment count are among the items included in the data. A category_id field, which differs by area, is also included in the JSON file linked to the region.

https://www.kaggle.com/datasets/datasnaek/youtube-new
----------------------------------------------------------------
# Architecture Diagram
----------------------------------------------------------------
![architecture](https://user-images.githubusercontent.com/100506830/229491097-a335fdcd-e70d-4e30-954f-20a066c73d78.jpeg)

Steps followed to complete the project
1. downloade the dataset from kraggle : https://www.kaggle.com/datasets/datasnaek/youtube-new?resource=download
2. downloade the Amazon CLI and Install it on the machine : https://aws.amazon.com/cli/
3. check by typing "aws" in command line if CLI has been installed.
4. Create IAM User for the Project and Set up password to login through IAM Credentials into AWS Management Console.
5. use Aws Command line to copy the data into S3 bucket.
6. Create S3 bucket to store data. and follow the Cli_command into terminal to execute the command and copy the data to s3.
7. Create IAM Role for the S3 bucket and Map it to S3.
8. Create lambda fuction for the S3 and test the code in lambdafunction.py
9. and for the rest follow this video link : https://www.youtube.com/watch?v=qFaaKme5eDE&list=PLBJe2dFI4sguF2nU6Z3Od7BX8eALZN3mU&index=2

----------------------------------------------------------------
# QuickSight Graph 
----------------------------------------------------------------
![Screenshot 2023-04-17 at 3 58 59 pm](https://user-images.githubusercontent.com/100506830/232397391-a5577175-ffb0-49b9-ab31-5122be351e6b.png)

videos related to People & Blog was the top most videos viewed.

pie chart for the similar data

![Screenshot 2023-04-17 at 4 19 30 pm](https://user-images.githubusercontent.com/100506830/232402296-1c2fc121-a2cb-44f6-968c-17350a42d34d.png)

views by country

Germany have highest views

![Screenshot 2023-04-17 at 4 29 55 pm](https://user-images.githubusercontent.com/100506830/232402651-65633454-bc88-4ff9-bfb6-5960970bf9c4.png)


final Dashboard for the data is:
[Sheet_1_2023-04-17T06_32_33.pdf](https://github.com/chamolipallav/Youtube-Data-Analysis/files/11246993/Sheet_1_2023-04-17T06_32_33.pdf)


