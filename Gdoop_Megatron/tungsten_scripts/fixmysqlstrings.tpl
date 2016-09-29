# /var/groupon/tung/install/tungsten_replicator/tungsten-replicator/samples/conf/filters/default/fixmysqlstrings.tpl
# Fixes up strings emanating from MySQL by converting them correctly to 
# Unicode string (char/varchar) or blob (binary/varbinary/blob data) depending 
# on the originating column type. 
#
# This filter must run after colnames and before pkey. 
replicator.filter.fixmysqlstrings=com.continuent.tungsten.replicator.filter.JavaScriptFilter                                
replicator.filter.fixmysqlstrings.script=${replicator.home.dir}/samples/extensions/javascript/fixmysqlstrings.js
replicator.filter.fixmysqlstrings.fieldtypes=BINARY,VARBINARY,TINYBLOB,BLOB,MEDIUMBLOB,LONGBLOB
