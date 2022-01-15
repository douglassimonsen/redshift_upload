SELECT distinct dependent_ns.nspname as child_schema,
                dependent_view.relname as child_view,
                dependent_view.relkind as child_kind,
                pg_views.viewowner,
                source_ns.nspname as parent_schema,
                source_table.relname as parent_table

FROM pg_depend

JOIN pg_rewrite 
ON pg_depend.objid = pg_rewrite.oid

JOIN pg_class as dependent_view 
ON pg_rewrite.ev_class = dependent_view.oid

JOIN pg_class as source_table 
ON pg_depend.refobjid = source_table.oid

JOIN pg_attribute 
ON pg_depend.refobjid = pg_attribute.attrelid
AND pg_depend.refobjsubid = pg_attribute.attnum

JOIN pg_namespace dependent_ns 
ON dependent_ns.oid = dependent_view.relnamespace

JOIN pg_namespace source_ns 
ON source_ns.oid = source_table.relnamespace

left JOIN pg_views 
on pg_views.schemaname = dependent_ns.nspname 
and pg_views.viewname = dependent_view.relname

WHERE source_ns.nspname = %(schema_name)s
AND source_table.relname = %(table_name)s
AND pg_attribute.attnum > 0
