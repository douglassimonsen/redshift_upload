create table upload_transactions (
  table_name varchar(200),
  upload_time timestamp,
  rows integer,
  redshift_user varchar(100),
  os_user varchar(100)
)
