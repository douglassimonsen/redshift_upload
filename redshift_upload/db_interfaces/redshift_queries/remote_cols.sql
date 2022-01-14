set search_path to {schema_name};

select "column" 

from pg_table_def

where schemaname = '{schema_name}'
and tablename = '{table_name}';