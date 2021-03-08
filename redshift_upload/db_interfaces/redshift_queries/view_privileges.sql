SELECT usename as username

FROM pg_views

cross join pg_user

WHERE HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'select') = true
and schemaname = %(schema_name)s
and viewname = %(view_name)s
