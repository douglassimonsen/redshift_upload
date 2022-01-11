SELECT usename as username,
       HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'select') select_permission,
       HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'insert') insert_permission,
       HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'update') update_permission,
       HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'delete') delete_permission,
       HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'references') references_permission

FROM pg_views

cross join pg_user

WHERE (
	HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'select') or
	HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'insert') or
	HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'update') or
	HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'delete') or
	HAS_TABLE_PRIVILEGE(pg_user.usename, schemaname + '.' + viewname, 'references')
)
and schemaname = %(schema_name)s
and viewname = %(view_name)s
