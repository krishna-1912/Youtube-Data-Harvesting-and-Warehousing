# Youtube-Data-Harvesting-and-Warehousing

This project uses the YouTube API to extract data from YouTube channels, playlists, and videos.

## Overview

This project aims to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels

## Prerequisites

- `Python` programming language
- `googleapiclient` library
- `pymongo` library
- `mysql-connector-python` library
- `streamlit` library
- YouTube Data API key (Get one from the Google Developers Console [https://console.developers.google.com/])

## MongoDb Connection

1. Install MongoDb and MongoDb Compass
2. mongodb://localhost:27017

## MySQL Connection

1. Install MySQL Server and MySQL Workbench
2. Create a local connection and a database

## Steps involved

1. Create API key and database connections
2. Extract data from youtube api using google api key and store it in MongoDb
3. Migration data from MongoDb to a SQL database for efficient querying and analysis
4. Search and retrieve data from SQL database using different search options
**Note:** Assumption is youtube_data_harvesting database is already created in MySQL

## Video Recording
https://github.com/krishna-1912/Youtube-Data-Harvesting-and-Warehousing/blob/main/Youtube%20Data%20Harvesting%20and%20Warehousing-1%20(1).mp4
https://github.com/krishna-1912/Youtube-Data-Harvesting-and-Warehousing/blob/main/Youtube%20Data%20Harvesting%20and%20Warehousing-1.mp4
