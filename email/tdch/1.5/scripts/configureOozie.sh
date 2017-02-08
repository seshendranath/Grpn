#!/bin/bash
# Copyright (C) 2016  by Teradata Corporation.
# All Rights Reserved.
#
# This script installs TDCH for Oozie transfers with Hadoop
#
# Hadoop distributions supported:
#   IBM BigInsights 3.0
#   IBM BigInsights 4.x (4.1 or later)
#   MapR 3.1
#   MapR 5.x (5.0 or later)
#   CDH 4.6
#   CDH 5.2
#   CDH 5.x (5.4 or later)
#   HDP 1.3
#   HDP 2.0
#   HDP 2.1
#   HDP 2.x (2.2 or later)

# Version : 1.5
TDCH_VERSION=1.5

#
# get active distro
#
DISTRO="unknown"
HV=$(hadoop version 2>&1)

# MAPR 3.1
if [[ $HV == *"mapr.com"* ]] && [[ $HV == *"Hadoop 1.0.3"* ]] ; then
  DISTRO="mapr3.1"
# MAPR 4.x
elif [[ $HV == *"mapr"* ]] && [[ $HV == *"Hadoop 2.5.1"* ]] ; then
  DISTRO="mapr4.x"
# MAPR 5.x (5.0 or later)
elif [[ $HV == *"mapr"* ]] && [[ $HV == *"Hadoop 2."* ]] ; then
  DISTRO="mapr5.x"
# IBM BigInsights 3.0
elif [[ $HV == *"/opt/ibm/biginsights"* ]] && [[ $HV == *"Hadoop 2.2.0"* ]] ; then
  DISTRO="ibm3.0"
# IBM BigInsights 4.x (4.1 or later)
elif [[ $HV == *"ibm.com"* ]] ; then
  DISTRO="ibm4.x"
# CDH 4.6
elif [[ $HV == *"cdh4.6"* ]] ; then
  DISTRO="cdh4.6"
# CDH 5.2
elif [[ $HV == *"cdh5.2"* ]] ; then
  DISTRO="cdh5.2"
# CDH 5.x (5.4 or later)
elif [[ $HV == *"cdh5."* ]] ; then
  DISTRO="cdh5.x"
# HDP 2.0
elif [[ $HV == *"hortonworks"* ]] && [[ $HV == *"Hadoop 2.2"* ]] ; then
  DISTRO="hdp2.0"
# HDP 2.1
elif [[ $HV == *"hortonworks"* ]] && [[ $HV == *"Hadoop 2.4"* ]] ; then
  DISTRO="hdp2.1"
# HDP 2.x (2.2 or later)
elif [[ $HV == *"hortonworks"* ]] ; then
  DISTRO="hdp2.x"
else
  DISTRO="hdp1.3"
fi

#
# set all distribution specific variables
# exit script if distribution not found
#
case "$DISTRO" in
  mapr3.1)

    DISTROUSAGE=$'[jt=jobTrackerHost] [oozie=oozieHost] [jtPort=jobTrackerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    jt - The Job Tracker host name (uses the cldbmaster node if omitted)\n    oozie - The Oozie host name (uses the cldbmaster node if omitted)\n    jtPort - The Job Tracker port number (50030 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses the cldbmaster node if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/opt/mapr/hive/hive-0.13"
    default_hcat_lib_dir="/opt/mapr/hive/hive-0.13/hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="mapr"
    distribution="MapR"
    distVersion="3.1"
    default_rm_port=8050
  ;;

  mapr4.x)

    DISTROUSAGE=$'[nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/opt/mapr/hive/hive-1.0"
    default_hcat_lib_dir="/opt/mapr/hive/hive-1.0/hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="mapr"
    distribution="MapR"
    distVersion="4.0"
    default_rm_port=8032
  ;;

  mapr5.x)

    DISTROUSAGE=$'[nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/opt/mapr/hive/hive-1.2"
    default_hcat_lib_dir="/opt/mapr/hive/hive-1.2/hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="mapr"
    distribution="MapR"
    distVersion="5.0"
    default_rm_port=8032
  ;;

  ibm3.0)

    DISTROUSAGE=$'nn=nameNodeHost [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/opt/ibm/biginsights/hive"
    default_hcat_lib_dir="/opt/ibm/biginsights/hive/hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="IBM"
    distVersion="3.0"
    default_rm_port=8050
  ;;

  ibm4.x)

    #
    # determine IBM version from /usr/iop directory
    #
    ibmversion=""
    for f in /usr/iop/4.*
    do
      ibmversion=`echo $f | sed -e 's/^.*iop\/\([0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`
    done

    DISTROUSAGE=$'nn=nameNodeHost [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/iop/4.*/hive"
    default_hcat_lib_dir="/usr/iop/4.*/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="IBM"
    distVersion=$ibmversion
    default_rm_port=8050
  ;;

  hdp1.3)

    source /etc/default/hadoop
    source /etc/default/hcatalog

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [jt=jobTrackerHost] [oozie=oozieHost] [nnPort=nameNodePortNum] [jtPort=jobTrackerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    jt - The Job Tracker host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    jtPort - The Job Tracker port number (50030 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/lib/hive"
    default_hcat_lib_dir="/usr/lib/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="HDP"
    distVersion="1.3"
    default_rm_port=8050
  ;;

  hdp2.0)

    source /etc/default/hadoop

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/lib/hive"
    default_hcat_lib_dir="/usr/lib/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="HDP"
    distVersion="2.0"
    default_rm_port=8050
  ;;

  hdp2.1)

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/lib/hive"
    default_hcat_lib_dir="/usr/lib/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir="/usr/lib/tez"
    user="hdfs"
    distribution="HDP"
    distVersion="2.1"
    default_rm_port=8050
  ;;

  hdp2.x)

    #
    # determine HDP version from /usr/hdp directory
    #
    hdpversion=""
    for f in /usr/hdp/2.*
    do
      hdpversion=`echo $f | sed -e 's/^.*hdp\/\([0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`
    done

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/hdp/2.*/hive"
    default_hcat_lib_dir="/usr/hdp/2.*/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir="/usr/hdp/2.*/tez"
    user="hdfs"
    distribution="HDP"
    distVersion=$hdpversion
    default_rm_port=8050
  ;;

  cdh4.6)

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [jt=jobTrackerHost] [oozie=oozieHost] [nnPort=nameNodePortNum] [jtPort=jobTrackerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    jt - The Job Tracker host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    jtPort - The Job Tracker port number (50030 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/lib/hive"
    default_hcat_lib_dir="/usr/lib/hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="CDH"
    distVersion="4.6"
    default_rm_port=8032
  ;;

  cdh5.2)

    source /etc/default/hadoop

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/usr/lib/hive"
    default_hcat_lib_dir="/usr/lib/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="CDH"
    distVersion="5.2"
    default_rm_port=8032
  ;;

  cdh5.x)

    #
    # determine CDH version from parcels directory
    #
    cdhversion=""
    for f in /var/opt/teradata/cloudera/parcels/CDH-5.*
    do
      cdhversion=`echo $f | sed -e 's/^.*CDH-\([0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`
    done

    if [ -f /etc/default/hadoop ] ; then
      source /etc/default/hadoop
    fi

    DISTROUSAGE=$'nn=nameNodeHost [nnHA=fs.default.value] [rm=resourceManagerHost] [oozie=oozieHost] [webhcat=webHCatHost] [webhdfs=webHdfsHost] [nnPort=nameNodePortNum] [rmPort=resourceManagerPortNum] [ooziePort=ooziePortNum] [webhcatPort=webhcatPortNum] [webhdfsPort=webhdfsPortNum] [hiveClientMetastorePort=hiveClientMetastorePortNum] [kerberosRealm=kerberosRealm] [hiveMetaStore=hiveMetaStoreHost] [hiveMetaStoreKerberosPrincipal=hiveMetaStoreKerberosPrincipal]\n\n    nn - The Name Node host name (required)\n    nnHA – If the name node is HA, specify the fs.defaultFS value found in ‘core-site.xml’\n    rm - The Resource Manager host name (uses nn parameter value if omitted)\n    oozie - The Oozie host name (uses nn parameter value if omitted)\n    webhcat - The WebHCatalog host name (uses nn parameter if omitted)\n    webhdfs - The WebHDFS host name (uses nn parameter if omitted)\n    nnPort - The Name node port number (8020 if omitted)\n    rmPort - The Resource Manager port number (8050 if omitted)\n    ooziePort - The Oozie port number (11000 if omitted)\n    webhcatPort - The WebHCatalog port number (50111 if omitted)\n    webhdfsPort - The WebHDFS port number (50070 if omitted)\n    hiveClientMetastorePort - The URI port for hive client to connect to metastore server (9083 if omitted)\n    kerberosRealm - name of the Kerberos realm\n    hiveMetaStore - The Hive Metastore host name (uses nn paarameter value if omitted)\n    hiveMetaStoreKerberosPrincipal - The service principal for the metastore thrift server (hive/_HOST if ommitted)'

    default_hive_lib_dir="/var/opt/teradata/cloudera/parcels/CDH-5.*/lib/hive"
    default_hcat_lib_dir="/var/opt/teradata/cloudera/parcels/CDH-5.*/lib/hive-hcatalog/share/hcatalog"
    default_tez_lib_dir=""
    user="hdfs"
    distribution="CDH"
    distVersion=$cdhversion
    default_rm_port=8032
  ;;

  unknown)

    echo "Hadoop distribution not supported."
    exit 1
  ;;

esac

#
# set HIVE_HOME environment variable (if not already set)
#
if [ -d $default_hive_lib_dir ] ; then
  hive_lib_dir=$default_hive_lib_dir
elif [ -d /usr/lib/hive ] ; then
  hive_lib_dir="/usr/lib/hive"
elif [ -z $HIVE_HOME ] ; then
  echo "Hive default directory not found."
  exit 1
fi
export HIVE_HOME=${HIVE_HOME:-$hive_lib_dir}

#
# set HCAT_HOME environment variable (if not already set)
#
if [ -d $default_hcat_lib_dir ] ; then
  hcat_lib_dir=$default_hcat_lib_dir
elif [ -d /usr/lib/hcatalog/share/hcatalog ] ; then
  hcat_lib_dir="/usr/lib/hcatalog/share/hcatalog"
elif [ -z $HCAT_HOME ] ; then
  echo "HCatalog default directory not found."
  exit 1
fi
export HCAT_HOME=${HCAT_HOME:-$hcat_lib_dir}

#
# set TEZ_HOME environment variable (only if default_tez_lib_dir is set)
#
# todo: if there are multiple directories under default_tez_lib_dir -d
#       check will return false; this probably needs to be fixed
tez_lib_dir=""
if [ -n $default_tez_lib_dir ] ; then
  if [ -d $default_tez_lib_dir ] ; then
    tez_lib_dir=$default_tez_lib_dir
  elif [ -z $TEZ_HOME ] ; then
    echo "Tez default directory not found; default was $default_tez_lib_dir."
    exit 1
  fi
fi
if [ -n $tez_lib_dir ] ; then
  export TEZ_HOME=${TEZ_HOME:-$tez_lib_dir}
fi

#
# function:    showUsageAndExit
# description: show command usage and exit script
#
function showUsageAndExit {
  USAGE="Usage: $0"
  echo "$USAGE" "$DISTROUSAGE"
  exit $1
}

#
# function:    makeint
# description: make integer value
#
function makeint {
  parts=($(echo $1 | tr "." "\n"))
  part0=${parts[0]}
  part1=${parts[1]}
  intvalue=$(( $part0*1000000 + $part1*1000 ))
}

#
# function:    findconnector
# description: determine if the TDCH is installed
#
function findconnector {
  numconnectors=`ls -1 /usr/lib/tdch/1.*/lib/teradata-connector-*.jar 2> /dev/null | wc -l`
  if [ $numconnectors -eq 0  ] ; then
    echo "ERROR: The Teradata Connector for Hadoop needs to be installed in /usr/lib/tdch"
    exit 3
  else
    connectorversion=0
    connectorjar=""
    versionpart=""
    for f in /usr/lib/tdch/1.*/lib/teradata-connector-*.jar
    do
      if [ -z "$connectorjar" ] ; then
        connectorjar=$f
      fi

      fullversionpart=`echo $f | sed -e 's/^.*teradata-connector-\([0-9][0-9]*\.[0-9][0-9]*.*\)\.jar$/\1/'`
      versionpart=`echo $f | sed -e 's/^.*teradata-connector-\([0-9][0-9]*\.[0-9][0-9]*\).*$/\1/'`

      if [ "$f" != "$versionpart" ] ; then
        makeint $versionpart

        if [ $intvalue -gt $connectorversion ] ; then
          fullconnectorversion=$fullversionpart
          connectorversion=$intvalue
          connectorjar=$f
        fi
      fi
    done

    if [ $connectorversion -lt 1003000 ] ; then
      echo "ERROR: The version of the Teradata Connector for Hadoop jar in /usr/lib/tdch needs to be 1.3.0 or greater"
      exit 4
    fi
  fi
}

#
# function:    findjava
# description: make sure java is installed
#
function findjava {
  local jarcmd=`ls -1 /usr/hadoop-jdk*/bin/jar 2> /dev/null | tail -1`
  if [ -z $jarcmd  ] ; then
    #try sandbox location of JDK
    jarcmd=`ls -1 /usr/jdk*/jdk*/bin/jar 2> /dev/null | tail -1`

    if [ -z $jarcmd ] ; then
      #try TDH location
      jarcmd=`ls -1 /opt/teradata/jvm64/jdk*/bin/jar 2> /dev/null | tail -1`

      if [ -z $jarcmd ] ; then
        #try CDH location
        jarcmd=`ls -1 /usr/java/jdk*/bin/jar 2> /dev/null | tail -1`

        if [ -z $jarcmd ] ; then
          #try MapR location
          jarcmd=`ls -1 /usr/lib/jvm/java*/bin/jar 2> /dev/null | tail -1`

          if [ -z $jarcmd ] ; then
            echo "ERROR: No Hadoop JDK jar command found"
            exit 5
          fi
        fi
      fi
    fi
  fi

  echo $jarcmd
}

nameNode=""
nameNodeHA=""
nameNodeHAConfigured="false"
resourceManager=""
jobTracker=""
oozieServer=""
webhcat=""
webhdfs=""
nnPort=8020
jtPort=9001
rmPort=$default_rm_port
ooziePort=11000
webhcatPort=50111
webhdfsPort=50070
hiveClientMetastorePort=9083
kerberosRealm=""
hiveMetaStore=""
hiveMetaStoreKerberosPrincipal="hive/_HOST"

#
# check to make sure at least one parameter was defined (unless on MapR which does not require the namenode parameter)
#
if [[ $DISTRO != *"mapr"* ]] && [[ ( $# < 1 ) ]] ; then
  echo No parameters specified, parameter \"nn\" is required.
  showUsageAndExit 6
fi

#
# loop through all parameters and set appropriate variables
#
while [[ $# > 0 ]] ; do
  arr=($(echo $1 | tr "=" "\n"))
  part0=${arr[0]}
  part1=${arr[1]}

  if [ -z "$part1" ]; then
    echo Unexpected parameter: $1
    showUsageAndExit 7
  fi

  case "$part0" in
    "nn")
      nameNode=$part1
    ;;
    "nnHA")
      nameNodeHA=$part1
      nameNodeHAConfigured="true"
    ;;
    "rm")
      resourceManager=$part1
    ;;
    "jt")
      jobTracker=$part1
    ;;
    "oozie")
      oozieServer=$part1
    ;;
    "webhcat")
      webhcat=$part1
    ;;
    "webhdfs")
      webhdfs=$part1
    ;;
    "nnPort")
      nnPort=$part1
    ;;
    "rmPort")
      rmPort=$part1
    ;;
    "jtPort")
      jtPort=$part1
    ;;
    "ooziePort")
      ooziePort=$part1
    ;;
    "webhcatPort")
      webhcatPort=$part1
    ;;
    "webhdfsPort")
      webhdfsPort=$part1
    ;;
    "hiveClientMetastorePort")
      hiveClientMetastorePort=$part1
    ;;
    "kerberosRealm")
      kerberosRealm=$part1
    ;;
    "hiveMetaStore")
      hiveMetaStore=$part1
    ;;
    "hiveMetaStoreKerberosPrincipal")
      hiveMetaStoreKerberosPrincipal=$part1
    ;;
    "tdchJar")
      connectorjar=$part1
    ;;
    *)
      echo Unexpected parameter: $1
      showUsageAndExit 8
    ;;
  esac
  shift
done

#
# find the tdch connector (and exit if not found or correct)
#
if [ -z $connectorjar ]; then
  findconnector
fi


#
# make sure that namenode is defined
#
if [ -z "$nameNode" ] ; then
  # for MAPR, set the name node to the cldbmaster
  if [[ $DISTRO == *"mapr"* ]] ; then
    nameNode=$(maprcli node cldbmaster | awk '{print $4;}' 2>&1)
    nameNode=$(echo $nameNode | tr -d '\r')
    # check for namenode again
    if [ -z "$nameNode" ] ; then
      echo Could not determine cldbmaster
      showUsageAndExit 9
    fi
  else
    echo Missing \"nn\" \(Name Node host\) parameter
    showUsageAndExit 9
  fi
fi

#
# set all to namenode if not defined
#
if [ -z "$oozieServer" ] ; then
  oozieServer=$nameNode
fi
if [ -z "$resourceManager" ] ; then
  resourceManager=$nameNode
fi
if [ -z "$webhcat" ] ; then
  webhcat=$nameNode
fi
if [ -z "$webhdfs" ] ; then
  webhdfs=$nameNode
fi
if [ -z "$jobTracker" ] ; then
  jobTracker=$nameNode
fi
if [ -z "$hiveMetaStore" ] ; then
  hiveMetaStore=$nameNode
fi

#
# test to make sure cluster is up and running
#
if [[ $DISTRO == *"mapr"* ]] ; then
  echo "Verifying cluster is up (maprcli node list)"
  su - mapr -c "maprcli node list"
  if [ $? -ne 0 ] ; then
    echo "ERROR: Please Verify Hadoop is up and running"
    exit 10
  fi
else
  echo  "Verifying cluster is up (dfsadmin -report)"
  su - hdfs -c "hdfs dfsadmin -report"
  if [ $? -ne 0 ] ; then
    echo "ERROR: Please Verify Hadoop is up and running"
    exit 10
  fi
fi

#
# function:    testhost
# description: test the host argument
#
function testhost {
  ping -c 1 $1 > /dev/null 2>&1
  if [ $? -ne 0 ] ; then
    echo Host defined by parameter $2 could not be reached.
    exit 2
  fi
}

#
# test all hosts (only test the host if it is not the same as the namenode)
#
testhost $nameNode nn
if [[ "$oozieServer" != "$nameNode" ]] ; then
  testhost $oozieServer oozie
fi
if [[ "$jobTracker" != "$nameNode" ]] ; then
  testhost $jobTracker jt
fi
if [[ "$resourceManager" != "$nameNode" ]] ; then
  testhost $resourceManager rm
fi
if [[ "$webhcat" != "$nameNode" ]] ; then
  testhost $webhcat webhcat
fi
if [[ "$webhdfs" != "$nameNode" ]] ; then
  testhost $webhdfs webhdfs
fi

#
# function:    testPortNumber
# description: check for valid port argument
#
function testPortNumber {
  case $1 in
    ''|*[!0-9]*)
      echo Invalid port number $1 for parameter $2
      showUsageAndExit 1
    ;;
    *)
      # do nothing
    ;;
  esac
}

#
# test all port numbers
#
testPortNumber $nnPort nnPort
testPortNumber $jtPort jtPort
testPortNumber $ooziePort ooziePort
testPortNumber $webhcatPort webhcatPort
testPortNumber $webhdfsPort webhdfsPort
testPortNumber $hiveClientMetastorePort hiveClientMetastorePort


#
# depending on the distro, use different remove command
#
rmcmd="-rm -r"
if [[ $DISTRO == "mapr3.1" ]] ; then
  rmcmd="-rmr"
fi

#
# create teradata directories in hdfs
#
echo "Creating teradata directories in HDFS"
if [[ $(su - $user -c "hadoop fs -ls /teradata/hadoop/lib" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs $rmcmd /teradata/hadoop/lib"
fi
if [[ $(su - $user -c "hadoop fs -ls /teradata/hadoop" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs $rmcmd /teradata/hadoop"
fi
if [[ $(su - $user -c "hadoop fs -ls /teradata/tdch/services.json" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs $rmcmd /teradata/tdch/services.json"
fi
if [[ $(su - $user -c "hadoop fs -ls /teradata/tdch/$TDCH_VERSION" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs $rmcmd /teradata/tdch/$TDCH_VERSION"
fi

if [[ $(su - $user -c "hadoop fs -ls /teradata" 2>&1) == *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs -mkdir /teradata"
fi

su - $user -c "hadoop fs -mkdir /teradata/hadoop"
su - $user -c "hadoop fs -mkdir /teradata/hadoop/lib"

if [[ $(su - $user -c "hadoop fs -ls /teradata/tdch" 2>&1) == *"No such file or directory"* ]] ; then
  su - $user -c "hadoop fs -mkdir /teradata/tdch"
fi

su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/lib"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieexport"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/ooziehadooptoteradata"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieteradatatoexistinghadoop"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieteradatatonewhadoop"
# Data Mover additions
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/jobOutputFolder"
su - $user -c "hadoop fs -mkdir /teradata/tdch/$TDCH_VERSION/oozieworkflows/hive"

#
# copy hive jars into /teradata/hadoop/lib
#
echo "Copying hive jars into /teradata/hadoop/lib"
su - $user -c "hadoop fs -put $HIVE_HOME/conf/hive-site.xml /teradata/hadoop/lib"

if [[ $DISTRO == "hdp2.x" ]] && [[ $(su - $user -c "hadoop fs -ls /user/oozie/share/lib/lib*/hive" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/libthrift-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/libfb303-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/hive-metastore-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/hive-exec-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/hive-cli-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/datanucleus-rdbms-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/datanucleus-core-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/antlr-runtime-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/jdo-api-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/commons-pool-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/commons-dbcp-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/datanucleus-api-jdo-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/hive-service-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/jline-*.jar /teradata/hadoop/lib"
else
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/libthrift-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/libfb303-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/hive-metastore-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/hive-exec-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/hive-cli-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/datanucleus-rdbms-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/datanucleus-core-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/antlr-runtime-*.jar /teradata/hadoop/lib"

  # CDH 4.6 / HDP 1.3 specific
  if [[ $DISTRO == "cdh4.6" ]] || [[ $DISTRO == "hdp1.3" ]] ; then
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/slf4j-api-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/jdo2-api-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/hive-builtins-*.jar /teradata/hadoop/lib"
  else
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/jdo-api-*.jar /teradata/hadoop/lib"
  fi

  # exclude MAPR 3.1
  if [[ $DISTRO != "mapr3.1" ]] ; then
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/commons-pool-*.jar /teradata/hadoop/lib"
    # exclude IBM 3.0
    if [[ $DISTRO != "ibm3.0" ]] ; then
      su - $user -c "hadoop fs -put $HIVE_HOME/lib/commons-dbcp-*.jar /teradata/hadoop/lib"
    fi
    su - $user -c "hadoop fs -put $HIVE_HOME/lib/datanucleus-api-jdo-*.jar /teradata/hadoop/lib"
  fi

  # Data Mover additions
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/hive-service-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/jline-*.jar /teradata/hadoop/lib"
fi

#
# copy hcatalog jars into /teradata/hadoop/lib
#
echo "Copying hcatalog jars into /teradata/hadoop/lib"
if [[ $DISTRO == "hdp2.x" ]] && [[ $(su - $user -c "hadoop fs -ls /user/oozie/share/lib/lib*/hcatalog" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hcatalog/*hcatalog-core-*.jar /teradata/hadoop/lib"
else
  #in some cases $HCAT_HOME points to hive-hcatalog/share/hcatalog
  if ls $HCAT_HOME/*hcatalog-core-*.jar 1> /dev/null 2>&1; then
    su - $user -c "hadoop fs -put $HCAT_HOME/*hcatalog-core-*.jar /teradata/hadoop/lib"
  else
    su - $user -c "hadoop fs -put $HCAT_HOME/share/hcatalog/*hcatalog-core-*.jar /teradata/hadoop/lib"
  fi
fi

#
# copy tez jars into /teradata/hadoop/lib
#
echo "Copying tez jars into /teradata/hadoop/lib"
if [[ -n "$default_tez_lib_dir" ]] ; then
  su - $user -c "hadoop fs -put /etc/tez/conf/tez-site.xml /teradata/hadoop/lib"
fi
if [[ $DISTRO == "hdp2.x" ]] && [[ $(su - $user -c "hadoop fs -ls /user/oozie/share/lib/lib*/hive" 2>&1) != *"No such file or directory"* ]] ; then
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/tez-api-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/tez-mapreduce-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/tez-yarn-timeline-history-with-acls-*.jar /teradata/hadoop/lib"
  su - $user -c "hadoop distcp /user/oozie/share/lib/lib*/hive/tez-yarn-timeline-history-with-fs-*.jar /teradata/hadoop/lib"
else
  if [[ -n "$default_tez_lib_dir" ]] ; then
    su - $user -c "hadoop fs -put $TEZ_HOME/tez-api-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put $TEZ_HOME/tez-mapreduce-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put $TEZ_HOME/tez-yarn-timeline-history-with-acls-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put $TEZ_HOME/tez-yarn-timeline-history-with-fs-*.jar /teradata/hadoop/lib"
  fi
fi

# mysql jar
mysqljars=`ls -1 $HIVE_HOME/lib/mysql-connector-*.jar 2> /dev/null`
if [ -n "$mysqljars" ] ; then
  echo "Copying mysql jars into /teradata/hadoop/lib"
  su - $user -c "hadoop fs -put $HIVE_HOME/lib/mysql-connector-*.jar /teradata/hadoop/lib"
fi

# atlas jars
if [ $DISTRO == "hdp2.x" ] ; then
  atlasjars=`ls -1 /usr/hdp/2.*/atlas/hook/hive/*.jar 2> /dev/null`
  if [ -n "$atlasjars" ] ; then
    echo "Copying atlas jars into /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/hive-bridge-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/atlas-typesystem-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/atlas-client-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/json4s-core_*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/json4s-ast_*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/json4s-native_*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/scala-library-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/scala-compiler-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/scala-reflect-*.jar /teradata/hadoop/lib"
    su - $user -c "hadoop fs -put /usr/hdp/2.*/atlas/hook/hive/scalap-*.jar /teradata/hadoop/lib"
  fi
fi  

PWD=$(pwd)

#
# copy TDCH jar to hdfs
#
echo "Copying Teradata Connector"
su - $user -c "hadoop fs -put $connectorjar /teradata/tdch/$TDCH_VERSION/lib"

#
# copy TDGSS and TERAJDBC jars
#
echo "Copying Teradata jars"

numgsslibs=`ls -1 /usr/lib/tdch/$versionpart/lib/tdgssconfig.jar 2> /dev/null | wc -l`
numjdbclibs=`ls -1 /usr/lib/tdch/$versionpart/lib/terajdbc*.jar 2> /dev/null | wc -l`

if [ $numgsslibs -eq 0 ] || [ $numjdbclibs -eq 0 ] ; then
  setupconnector=/tmp/setupconnector
  rm -rf $setupconnector
  mkdir $setupconnector
  cd $setupconnector
  jarcmd=$(findjava)
  ${jarcmd} -xf $connectorjar
  su - $user -c "hadoop fs -put $setupconnector/lib/* /teradata/tdch/$TDCH_VERSION/lib"
  cd $PWD
  rm -rf $setupsconnector
else
  su - $user -c "hadoop fs -put /usr/lib/tdch/$versionpart/lib/tdgssconfig.jar /teradata/tdch/$TDCH_VERSION/lib"
  su - $user -c "hadoop fs -put /usr/lib/tdch/$versionpart/lib/terajdbc*.jar /teradata/tdch/$TDCH_VERSION/lib"
fi

#
# create services.json
#
rmanager=$resourceManager
rmanagerPort=$rmPort
nn=$nameNode
if [[ $DISTRO == *"mapr3"* ]] ; then
  rmanager="maprfs://"
  rmanagerPort=$jtPort
  nn="maprfs://"
elif [[ $DISTRO == *"mapr"* ]] ; then
  nn="maprfs://"
elif [[ $DISTRO == "hdp1.3" ]] || [[ $DISTRO == "cdh4.6" ]] ; then
  rmanager=$jobTracker
  rmanagerPort=$jtPort
fi

cat<< XXX > /tmp/services.json
{
        "Distribution":"$distribution",
        "DistributionVersion":"$distVersion",
        "TeradataConnectorForHadoopVersion":"$fullconnectorversion",
        "WebHCatalog":"$webhcat",
        "WebHCatalogPort":$webhcatPort,
        "WebHDFS":"$webhdfs",
        "WebHDFSPort":$webhdfsPort,
        "JobTracker":"$rmanager",
        "JobTrackerPort":$rmanagerPort,
        "NameNode":"$nn",
        "NameNodePort":$nnPort,
        "NameNodeHA":"$nameNodeHA",
        "NameNodeHAConfigured":$nameNodeHAConfigured,
        "Oozie":"$oozieServer",
        "OoziePort":$ooziePort,
        "HiveClientMetastorePort":$hiveClientMetastorePort,
        "HiveMetaStoreKerberosPrincipal":"$hiveMetaStoreKerberosPrincipal",
        "KerberosRealm":"$kerberosRealm",
        "HiveMetaStore":"$hiveMetaStore"
}
XXX

echo The following is the specification of the Hadoop services used by the Oozie workflows:
cat /tmp/services.json
echo
echo

#
# create workflows
#
echo "Creating Oozie workflow xml files"

cat<<XXX > /tmp/exportworkflow.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataExportTool-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorExportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-batchsize</arg>
            <arg>10000</arg>
            <arg>-sourcedatabase</arg>
            <arg>\${sourceTableDatabase}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveconf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/importworkflow.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportTool-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-batchsize</arg>
            <arg>10000</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumnName}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetTableDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveconf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/hdfsimportworkflow.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportTool-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-jobtype</arg>
            <arg>hdfs</arg>
            <arg>-fileformat</arg>
            <arg>textfile</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targetpaths</arg>
            <arg>\${targetPaths}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/ooziehadooptoteradata.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataExportToolByFields-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorExportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-batchsize</arg>
            <arg>10000</arg>
            <arg>-sourcedatabase</arg>
            <arg>\${sourceTableDatabase}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceColumnNames}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetColumnNames}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveconf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/oozieteradatatoexistinghadoop.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolToExisting-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-batchsize</arg>
            <arg>10000</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumnName}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetTableDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveconf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/oozieteradatatonewhadoop.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libjars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${jdbcURL}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-batchsize</arg>
            <arg>10000</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumnName}</arg>
            <arg>-sourcequery</arg>
            <arg>\${sourceQuery}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetTableDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveconf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobOutputFile}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

#DATA MOVER WORKFLOW FILES
#Import Files
cat<<XXX > /tmp/ooziedmteradatatohadooptableexists.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/ooziedmteradatatohadoopnewtablenonppi.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
       	    <arg>-targetfieldnames</arg>
    	    <arg>\${targetFieldNames}</arg>
    	    <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/ooziedmteradatatohadoopnewtableppi.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetpartitionschema</arg>
            <arg>\${targetPartitionSchema}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/ooziedmteradatatohadoopnewtablepartial.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcequery</arg>
            <arg>\${sourceQuery}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/ooziedmteradatatohadooptableexistpartial.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataImportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcequery</arg>
            <arg>\${sourceQuery}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

#Export Files
cat<<XXX > /tmp/ooziedmhadooptoteradatatableexists.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="TeradataExportToolByQuery-wf">
    <start to="java-node"/>
    <action name="java-node">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorExportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-sourcedatabase</arg>
            <arg>\${sourceDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-errorlimit</arg>
            <arg>1</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
	    </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

# Kerberos workflows (only generate if kerberosRealm is defined)
if [ ${#kerberosRealm} -ne 0 ] ; then

cat<<XXX > /tmp/krb_ooziedmhadooptoteradatatableexists.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.3" name="TeradataExportToolByQuery-wf">
    <credentials>
        <credential name="hive_credentials" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcatUri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcatPrincipal}</value>
            </property>
        </credential>
    </credentials>
    <start to="java-node"/>
    <action name="java-node" cred="hive_credentials">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorExportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-sourcedatabase</arg>
            <arg>\${sourceDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-errorlimit</arg>
            <arg>1</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/krb_ooziedmteradatatohadoopnewtablenonppi.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.3" name="TeradataImportToolByQuery-wf">
    <credentials>
        <credential name="hive_credentials" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcatUri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcatPrincipal}</value>
            </property>
        </credential>
    </credentials>
    <start to="java-node"/>
    <action name="java-node" cred="hive_credentials">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
       	    <arg>-targetfieldnames</arg>
    	    <arg>\${targetFieldNames}</arg>
    	    <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/krb_ooziedmteradatatohadooptableexists.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.3" name="TeradataImportToolByQuery-wf">
    <credentials>
        <credential name="hive_credentials" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcatUri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcatPrincipal}</value>
            </property>
        </credential>
    </credentials>
    <start to="java-node"/>
    <action name="java-node" cred="hive_credentials">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcetable</arg>
            <arg>\${sourceTable}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/krb_ooziedmteradatatohadooptableexistpartial.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.3" name="TeradataImportToolByQuery-wf">
    <credentials>
        <credential name="hive_credentials" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcatUri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcatPrincipal}</value>
            </property>
        </credential>
    </credentials>
    <start to="java-node"/>
    <action name="java-node" cred="hive_credentials">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcequery</arg>
            <arg>\${sourceQuery}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-targetfieldnames</arg>
            <arg>\${targetFieldNames}</arg>
            <arg>-sourcefieldnames</arg>
            <arg>\${sourceFieldNames}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/krb_ooziedmteradatatohadoopnewtablepartial.xml
<!--
Copyright (C) 2016  by Teradata Corporation.  All rights reserved.
-->
<workflow-app xmlns="uri:oozie:workflow:0.3" name="TeradataImportToolByQuery-wf">
    <credentials>
        <credential name="hive_credentials" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcatUri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcatPrincipal}</value>
            </property>
        </credential>
    </credentials>
    <start to="java-node"/>
    <action name="java-node" cred="hive_credentials">
        <java>
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
                <property>
                    <name>oozie.launcher.mapred.child.java.opts</name>
                    <value>-Djava.security.egd=file:/dev/./urandom</value>
                </property>
            </configuration>
            <main-class>com.teradata.connector.common.tool.ConnectorImportTool</main-class>
            <java-opts>-Xmx1024m -Djava.security.egd=file:/dev/./urandom</java-opts>
            <arg>-libjars</arg>
            <arg>\${libJars}</arg>
            <arg>-jobtype</arg>
            <arg>hive</arg>
            <arg>-fileformat</arg>
            <arg>\${fileFormat}</arg>
            <arg>-separator</arg>
            <arg>\${separator}</arg>
            <arg>-classname</arg>
            <arg>com.teradata.jdbc.TeraDriver</arg>
            <arg>-url</arg>
            <arg>\${url}</arg>
            <arg>-username</arg>
            <arg>\${userName}</arg>
            <arg>-password</arg>
            <arg>\${password}</arg>
            <arg>-method</arg>
            <arg>\${method}</arg>
            <arg>-nummappers</arg>
            <arg>\${numMappers}</arg>
            <arg>-batchsize</arg>
            <arg>\${batchSize}</arg>
            <arg>-sourcequery</arg>
            <arg>\${sourceQuery}</arg>
            <arg>-targettableschema</arg>
            <arg>\${targetTableSchema}</arg>
            <arg>-targetdatabase</arg>
            <arg>\${targetDatabase}</arg>
            <arg>-targettable</arg>
            <arg>\${targetTable}</arg>
            <arg>-hiveconf</arg>
            <arg>\${hiveConf}</arg>
            <arg>-jobclientoutput</arg>
            <arg>\${jobClientOutput}</arg>
            <arg>-splitbycolumn</arg>
            <arg>\${splitByColumn}</arg>
            <arg>-usexviews</arg>
            <arg>\${useXViews}</arg>
            <arg>-upt</arg>
            <arg>\${upt}</arg>
        </java>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Java failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

fi

# Hive Files
cat<<XXX > /tmp/ooziedmhiveworkflow.xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<workflow-app xmlns="uri:oozie:workflow:0.2" name="hive-wf">
    <start to="hive-node"/>

    <action name="hive-node">
        <hive xmlns="uri:oozie:hive-action:0.2">
            <job-tracker>\${jobTracker}</job-tracker>
            <name-node>\${nameNode}</name-node>
            <job-xml>\${hiveConf}</job-xml>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>\${queueName}</value>
                </property>
            </configuration>
            <script>\${script}</script>
            <param>DATABASE=\${database}</param>
            <param>TABLE=\${table}</param>
            <param>OUTPUT=\${jobClientOutput}</param>
        </hive>
        <ok to="end"/>
        <error to="fail"/>
    </action>

    <kill name="fail">
        <message>Hive failed, error message[\${wf:errorMessage(wf:lastErrorNode())}]</message>
    </kill>
    <end name="end"/>
</workflow-app>
XXX

cat<<XXX > /tmp/dmhivescripttruncate.sql
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
use \${DATABASE};
truncate table \${TABLE};
XXX

#
# copy oozie workflow xml files to hdfs
#
echo "Copy Oozie workflow xml files into hdfs"
su - $user -c "hadoop fs -put /tmp/exportworkflow.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieexport/workflow.xml"
su - $user -c "hadoop fs -put /tmp/importworkflow.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow.xml"
su - $user -c "hadoop fs -put /tmp/hdfsimportworkflow.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/hdfsworkflow.xml"
su - $user -c "hadoop fs -put /tmp/ooziehadooptoteradata.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/ooziehadooptoteradata/workflow.xml"
su - $user -c "hadoop fs -put /tmp/oozieteradatatoexistinghadoop.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieteradatatoexistinghadoop/workflow.xml"
su - $user -c "hadoop fs -put /tmp/oozieteradatatonewhadoop.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieteradatatonewhadoop/workflow.xml"
su - $user -c "hadoop fs -put /tmp/services.json /teradata/tdch/"
#Data Mover addition
su - $user -c "hadoop fs -put /tmp/ooziedmteradatatohadooptableexists.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow_dm_table_exist.xml"
su - $user -c "hadoop fs -put /tmp/ooziedmteradatatohadoopnewtablenonppi.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow_dm_table_new_non_ppi.xml"
su - $user -c "hadoop fs -put /tmp/ooziedmteradatatohadoopnewtableppi.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow_dm_table_new_ppi.xml"
su - $user -c "hadoop fs -put /tmp/ooziedmteradatatohadoopnewtablepartial.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow_dm_table_new_partial.xml"
su - $user -c "hadoop fs -put /tmp/ooziedmteradatatohadooptableexistpartial.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/workflow_dm_table_exist_partial.xml"
su - $user -c "hadoop fs -put /tmp/ooziedmhiveworkflow.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/hive/workflow.xml"
su - $user -c "hadoop fs -put /tmp/dmhivescripttruncate.sql /teradata/tdch/$TDCH_VERSION/oozieworkflows/hive/script_truncate.q"
su - $user -c "hadoop fs -put /tmp/ooziedmhadooptoteradatatableexists.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieexport/workflow_dm_table_exist.xml"

if [ ${#kerberosRealm} -ne 0 ] ; then
  su - $user -c "hadoop fs -put /tmp/krb_ooziedmhadooptoteradatatableexists.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieexport/krb_workflow_dm_table_exist.xml"
  su - $user -c "hadoop fs -put /tmp/krb_ooziedmteradatatohadoopnewtablenonppi.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/krb_workflow_dm_table_new_non_ppi.xml"
  su - $user -c "hadoop fs -put /tmp/krb_ooziedmteradatatohadooptableexists.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/krb_workflow_dm_table_exist.xml"
  su - $user -c "hadoop fs -put /tmp/krb_ooziedmteradatatohadooptableexistpartial.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/krb_workflow_dm_table_exist_partial.xml"
  su - $user -c "hadoop fs -put /tmp/krb_ooziedmteradatatohadoopnewtablepartial.xml /teradata/tdch/$TDCH_VERSION/oozieworkflows/oozieimport/krb_workflow_dm_table_new_partial.xml"
fi

su - $user -c "hadoop fs -chmod -R 755 /teradata"
#Data Mover addition
su - $user -c "hadoop fs -chmod -R 777 /teradata/tdch/$TDCH_VERSION/oozieworkflows/jobOutputFolder"

echo "All Done"
exit 0
