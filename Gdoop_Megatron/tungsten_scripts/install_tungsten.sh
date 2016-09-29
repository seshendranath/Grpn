###################################################
##TUNGSTEN INSTALLATION SCRIPT

##### sudo /var/tmp/roll hostclass.yml host.yml  -- Roll the host to set Hadoop, Hive and other configurations
##### sudo download_encap tungsten_replicator-3.0.0.526.20160129 -- Install appropriate Tungsten package use epkg if already downloaded
##### sudo download_encap java-1.8.0_72 -- Install appropriate Java package, use epkg if already downloaded
###################################################

#Checking whether the parameters are passed correctly
if [[ $# -lt 1 ]] || [[ "$1" != "master" &&  "$1" != "slave" ]];
then
        echo "Kindly specify right parameters, 1. master or slave"
        exit 1
fi


#Creating required Directories
#sudo rm -rf /etc/tungsten
#sudo mkdir /etc/tungsten
#sudo chown svc_meg_prod:svc_meg_prod /etc/tungsten

#sudo rm -rf /var/groupon/tung
#sudo mkdir /var/groupon/tung
#sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung

mkdir /var/groupon/tung/temp
#sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung/temp

#sudo rm -rf /var/groupon/tung_meta
#sudo mkdir /var/groupon/tung_meta
#sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung_meta

mkdir /var/groupon/tung/install
#sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung/install

#Extracting Tungsten jar in Install Location
curl -O http://config/package/tungsten_replicator-3.0.0.526.20160202.tar.gz
tar xzvf tungsten_replicator-3.0.0.526.20160202.tar.gz
#tar -C /var/groupon/tung/install -zxf tungsten_replicator-3.0.0.526.20160202/lib/tungsten_replicator.tar.gz
cp -R tungsten_replicator-3.0.0.526.20160202/lib/tungsten_replicator /var/groupon/tung/install/
#sudo chown -R svc_meg_prod:svc_meg_prod /var/groupon/tung/install/tungsten_replicator
chmod 777 /var/groupon/tung/install/tungsten_replicator

rm -rf /tmp/tungsten-configure.log

#Setting up limits file
#cat <<EOF | sudo tee --append /etc/security/limits.conf
# from /usr/local/config/C99tungsten_config:27
#tungsten    -    nofile    65535
## from /usr/local/config/C99tungsten_config:28
#mysql       -    nofile    65535
# from /usr/local/config/C99tungsten_config:29
#tungsten    -    nproc    8096
# from /usr/local/config/C99tungsten_config:30
#mysql       -    nproc    8096
#EOF


if [ $1 == 'master' ]; then
#Setup MASTER TUNGSTEN.INI Properties
cat <<EOF | tee --append /etc/tungsten/tungsten.ini
[defaults]
install-directory=/var/groupon/tung
java-file-encoding=UTF8
java-user-timezone=GMT
mysql-enable-enumtostring=true
mysql-enable-settostring=true
mysql-use-bytes-for-string=false
property=replicator.filter.pkey.addColumnsToDeletes=true
property=replicator.filter.pkey.addPkeyToInserts=true
property=replicator.extractor.dbms.usingBytesForString=true
replication-password=TungSt3n!
replication-user=tungsten
rmi-port=10005
skip-validation-check=HostsFileCheck
skip-validation-check=MySQLNoMySQLReplicationCheck
skip-validation-check=MySQLPermissionsCheck
skip-validation-check=MySQLSettingsCheck
skip-validation-check=ReplicationServicePipelines
skip-validation-check=THLStorageCheck
svc-extractor-filters=colnames,fixmysqlstrings,pkey,schemachange
auto-recovery-delay-interval=3m
auto-recovery-max-attempts=5
auto-recovery-reset-interval=30m
thl-log-retention=4d
temp-directory=/var/groupon/tung/temp
user=svc_meg_prod
EOF

else
#Setup SLAVE TUNGSTEN.INI Properties
cat <<EOF | tee --append /etc/tungsten/tungsten.ini
[defaults]
property=replicator.applier.dbms.parallelization=20
property=replicator.applier.dbms.partitionBy=tungsten_commit_timestamp
property=replicator.applier.dbms.partitionByClass=com.continuent.tungsten.replicator.applier.batch.DateTimeValuePartitioner
property=replicator.applier.dbms.partitionByFormat='ds='yyyy-MM-dd'/hr='HH
property=replicator.datasource.global.directory=/var/groupon/tung_meta
property=replicator.stage.q-to-dbms.blockCommitInterval=600s
property=replicator.stage.q-to-dbms.blockCommitPolicy=lax
property=replicator.stage.q-to-dbms.blockCommitRowCount=2000000
thl-log-retention=3d
property=replicator.filter.monitorschemachange.notify=true
property=replicator.applier.dbms.useUpdateOpcode=true
batch-enabled=true
batch-load-language=js
batch-load-template=hadoop
datasource-type=file
install-directory=/var/groupon/tung
java-file-encoding=UTF8
property=replicator.datasource.global.csvType=hive
rmi-port=10005
skip-validation-check=DatasourceDBPort
skip-validation-check=DirectDatasourceDBPort
skip-validation-check=HostsFileCheck
skip-validation-check=InstallerMasterSlaveCheck
auto-recovery-delay-interval=3m
auto-recovery-max-attempts=5
auto-recovery-reset-interval=30m
svc-applier-filters=schemachange,monitorschemachange
temp-directory=/var/groupon/tung/temp
user=svc_meg_prod
[groupon_orders_snc1]
master=flumecollector3
members=gdoop-megatron-slave1
dataservice-thl-port=2118
property=replicator.master.connect.uri=thl://flumecollector3:2118/
property=replicator.applier.dbms.stageDirectory=/var/groupon/tung/temp/staging/groupon_orders_snc1
EOF
fi

#sudo chown svc_meg_prod:svc_meg_prod /etc/tungsten/tungsten.ini

#If replicator not not found add "export PATH=$PATH:$HOME/bin:/var/groupon/tung/tungsten/tungsten-replicator/bin" to ~/.bashrc file 
#Running Replicator
/var/groupon/tung/install/tungsten_replicator/tools/tpm update
replicator start

#LOG: If there are any errors check /var/groupon/tung/tungsten/tungsten-replicator/log/trepsvc.log
