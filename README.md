# Project: Data Pipelines with Airflow

This project will use the core concepts of Apache Airflow. To complete the project, I need to create custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

## Overview

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring me into the project and expect me to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

 To complete the project, Custom operators are created to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

 ## Example DAG

<p align="center">
<img src="https://github.com/ashishp98/Data-Engineering-Nanodegree-P5-Data-Pipelines-with-Airflow/blob/main/images/Example_DAG.png"  >
</p>