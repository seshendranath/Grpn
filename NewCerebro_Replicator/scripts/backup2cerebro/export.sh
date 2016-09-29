#!/bin/bash

--./hdfs_exim_V1_NEW.pl --export --debug --table prod_groupondw,dim_marketing_parameters  --prefix /td_backup/TXT_LZO  --namenode hdfs://cerebro-namenode.snc1:8020 --source 'tdwc/ccloghin_dba,Vanilla!8888' --removedelimiters

./hdfs_exim_V1_NEW.pl --export --debug --table prod_groupondw,dim_marketing_parameters  --prefix /td_backup/TXT_LZO  --namenode hdfs://cerebro-namenode.snc1:8020 --source 'tdwc/ccloghin_dba,Vanilla!8888'
