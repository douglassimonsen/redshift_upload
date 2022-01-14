select L.lock_owner_pid pid, 
       S.user_name 

from stv_locks L

left join SVV_TABLE_INFO T
on T.table_id = L.table_id

left join STV_SESSIONS S
on S.process = l.lock_owner_pid

where T.table = %(table_name)s
and T.schema = %(schema_name)s
