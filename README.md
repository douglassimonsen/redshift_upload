The uploader goes through the following steps:
--1) Loads the Data
2) Fleshes out the parameters and checks for coherence
3) Persist a single connection
--4) Expand varchar columns if necessary
--3) Check whether table already exists
--3) Initializes an interface
--3) Gets column types
--4) Serializes the columns to proper string representations. Handles missing columns
--5) Logs dependent views (if complete refresh)
--6) Loads to S3
--7) Kill connections touching the table
--8) Copy data to table
--9) Reuploads dependent views (if complete refresh)
--10) Records the upload
11) Create a topological ordering for the views to be reinstantiated
12) Make the varchar length closer to the actual max string length
13) Create a better locking functions
14) Lock around editing the varchar columns as well
