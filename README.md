The uploader goes through the following steps:
--1) Loads the Data
2) Fleshes out the parameters and checks for coherence
3) Initializes an interface
--3) Gets column types
--4) Serializes the columns to proper string representations. Handles missing columns
5) Logs dependent views (if complete refresh)
6) Loads to S3
7) Kill connections touching the table
8) Copy data to table
9) Reuploads dependent views (if complete refresh)
10) Records the upload
