###################################################
##TUNGSTEN INSTALLATION SCRIPT

##### sudo /var/tmp/roll hostclass.yml host.yml  -- Roll the host to set Hadoop, Hive and other configurations
##### sudo download_encap tungsten_replicator-3.0.0.526.20160129 -- Install appropriate Tungsten package use epkg if already downloaded
##### sudo download_encap java-1.8.0_72 -- Install appropriate Java package, use epkg if already downloaded
###################################################

#Creating required Directories
sudo rm -rf /etc/tungsten
sudo mkdir /etc/tungsten
sudo chown svc_meg_prod:svc_meg_prod /etc/tungsten

sudo rm -rf /var/groupon/tung
sudo mkdir /var/groupon/tung
sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung

sudo rm -rf /var/groupon/tung_meta
sudo mkdir /var/groupon/tung_meta
sudo chown svc_meg_prod:svc_meg_prod /var/groupon/tung_meta

#Setting up limits file
cat <<EOF | sudo tee --append /etc/security/limits.conf
# from /usr/local/config/C99tungsten_config:27
tungsten    -    nofile    65535
# from /usr/local/config/C99tungsten_config:28
mysql       -    nofile    65535
# from /usr/local/config/C99tungsten_config:29
tungsten    -    nproc    8096
# from /usr/local/config/C99tungsten_config:30
mysql       -    nproc    8096
EOF

sudo su - svc_meg_prod -c 'sh install_tungsten.sh slave'
