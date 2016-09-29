#!/usr/local/bin/perl

use strict;
use Getopt::Long;
use POSIX ();
use POSIX qw(setsid);
use POSIX qw(:errno_h :fcntl_h);

my ($source,$table,$export,$import,$help,$rc,$debug,$target,$import,$prefix,$importschema,$stagingschema,$date,$namenode,$prefix_original);
my $SPOOL_LIMIT = 22000 ; #in MB LZO compressed
my $SQL_SELECTOR_MAX = 500 ; #in MB LZO compressed

#process comand line variables
GetOptions(
    'source=s' => \$source,
    'target=s' => \$target,
    'table=s'  => \$table,
    'export'   => \$export,
    'import'   => \$import,
    'importschema=s' => \$importschema,
    'stagingschema=s' => \$stagingschema,
    'date=s' => \$date,
    'prefix=s' => \$prefix,
    'help'     => \$help,
    'debug'    => \$debug,
    'namenode=s' => \$namenode
) or usage();
usage() if ($help or !$source);

chomp(my $dt=`date '+%Y%m%d%H%M%S'`);
print "$dt|";

$0="$0  --table $table";

my ($t_tpdid,$t_userid,$t_pass);
if ($target) {
	$target =~ /(.*)\/(.*),(.*)/ or usage("====>invalid target connect string $target \n ");
	$t_tpdid  = $1;
	$t_userid = $2;
	$t_pass   = $3;
	$0 = "$0 --target $t_tpdid/$t_userid";
} 

$namenode = "hdfs://cerebro-namenode.snc1:8020" if (!$namenode);

my ($s_tpdid,$s_userid,$s_pass);
if ($source) {
	$source =~ /(.*)\/(.*),(.*)/ or usage("====>invalid source connect string $source \n ");
	$s_tpdid  = $1;
	$s_userid = $2;
	$s_pass   = $3;
	$0 = "$0 --source $s_tpdid/$s_userid";
}


	#get a line of work to do, either a full tble export or an incremental depending on line format 
	#a full table would only contain $Tschema,$Ttable and an incremental would also have $increm_key,$increm_value
	
	
	my ($Tschema,$Ttable,$increm_key,$increm_value_original) = ('','',0,0);		
	($Tschema,$Ttable,$increm_key,$increm_value_original) = split(/,/,$table);

	#use increm_value for string that have non integer characters stripped out. This should be used for file names, paths purposes
	my $increm_value = $increm_value_original;
	$increm_value  =~ s/\D//g;


	#get current date, used for HDFS filename below
	my $currentday=`date '+%Y%m%d'`; chomp($currentday);
	$increm_value ="D" if (! $increm_key);
	my $schematable = "${Tschema}.${Ttable}.${increm_value}.${currentday}"; 		$schematable =~ s/[-]//g;

	#log file location to exportlogs/${schematable}.log
        my $logfile="exportlogs/${schematable}.log";    #log file location
        unless(-e "exportlogs" or mkdir "exportlogs" ) { die "Unable to create logs\n" } ;                      #create logs folder if it does not exist

        open my $TEE, '|-', "tee ${logfile}"  or die "Can't redirect STDOUT: $!";
        close (STDERR);open STDERR, ">&STDOUT" or die "Can't dup for STDERR: $!";
        select $TEE; #default file handle

	my $hdfsfolder;
	my $hdfsfile;
	if ($increm_key) {
		 $hdfsfolder = "${prefix}/${Tschema}.${Ttable}/${increm_value}";
		 $hdfsfile   = "${Tschema}.${Ttable}";
	} else {
		 if ($date ne "") {
		 		$hdfsfolder = "${prefix}/${Tschema}.${Ttable}/${date}";
		 } else {
				$hdfsfolder = "${prefix}/${Tschema}.${Ttable}/${currentday}";
		 }
		 $hdfsfile   = "${Tschema}.${Ttable}.D";
	}

	#BTEQ header / footer
	my $header=".SET RECORDMODE OFF\n .SET WIDTH 5000 \n .set separator '|' \n .set TITLEDASHES OFF \n .set ECHOREQ OFF \n  .LOGON " . $source . "\n";
	my $footer=".EXPORT RESET;\n.QUIT";

	my $prior_list=`hadoop fs -ls ${prefix}/${Tschema}.${Ttable} 2>/dev/null|tail -5|awk '{print \$8}'`;
	$prior_list =~ s/\n/ /g;

        my $prior_size = 0 ;
	if ($prior_list) {	
		$prior_size=`hadoop fs -du -s ${prior_list} |awk 'max=(max>\$1)?max:\$1; END {print max/1024/1024}' | tail -1`;
	}
	print "Prior_size $prior_size\n" if ($debug);

	#create manifest file on current node
	unlink("$schematable.manifest") if (-e "$schematable.manifest");
	my $where = " WHERE $increm_key=${increm_value_original}" if ($increm_key) ;
	my $sql = ".EXPORT DATA FILE = $schematable.manifest \n $header \n LOCK row for access select current_timestamp,CAST(count(*) AS BIGINT) from ${Tschema}.${Ttable} $where; \nSHOW TABLE ${Tschema}.${Ttable};\n $footer";
	print "Creating manifest file $schematable.manifest ....\n" if ($debug);


	`echo "$sql" | bteq -c UTF8 `  or die "Failure to get manifest for ${Tschema}.${Ttable} : $!";
	#check for missing table and abort work , cleanup
	$rc=$? >> 8;
        POSIX::_exit(1) if ($rc);
	#exit 1 if ($rc);

	unlink("$schematable.columns") if (-e "$schematable.columns");
	my $sql = ".EXPORT DATA FILE = $schematable.columns \n $header HELP TABLE ${Tschema}.${Ttable};\n $footer";
	print "Creating columns file $schematable.columns ... \n" if ($debug);

	`echo "$sql" | bteq -c UTF8 `  or die "Failure to get DDL for ${Tschema}.${Ttable} : $!";
	
	open(TABLEDDL,"$schematable.columns");
	my ($collist,$collist2, $collist3) = "";
	my $i = 0;
	my $len;
	my @intervals = qw(DH DM DS DY HM HR HS MI MO MS SC YM YR);
while(<TABLEDDL>) {    
	s/\s*|,//g;
	
	my @coltokens = split(/\|/,$_);
	$coltokens[0] = "\"$coltokens[0]\""; # need to enclose in doublequotes to avoid increm_keyword collisions in TPT
	$i++;
	$collist3 .=":COLUMN$i,";
	if (   ($coltokens[1] eq "CV") 
	    || ($coltokens[1] eq "CF")
	    || ($coltokens[1] eq "BF")
	    || ($coltokens[1] eq "BV") 
	   ) { 
				$len = $coltokens[4];
				$len =~ s/[X()]+//g; #extract number for x(ddd) column format
		$collist  .="COLUMN$i VARCHAR(".(3 * $len)."),\n"; $collist2 .="\nCAST($coltokens[0] AS VARCHAR(".$len.")),"; 
	} 
	if ($coltokens[1] eq "I" )  { $collist  .="COLUMN$i VARCHAR(33),\n";	$collist2 .="\nCAST($coltokens[0] AS VARCHAR(11)),"; } 
	if ($coltokens[1] eq "I1" ) { $collist  .="COLUMN$i VARCHAR(12),\n";    $collist2 .="\nCAST($coltokens[0] AS VARCHAR(4)),"; }
	if ($coltokens[1] eq "I2" ) { $collist  .="COLUMN$i VARCHAR(15),\n";    $collist2 .="\nCAST($coltokens[0] AS VARCHAR(5)),"; }
	if ($coltokens[1] eq "I8" ) { $collist  .="COLUMN$i VARCHAR(60),\n";    $collist2 .="\nCAST($coltokens[0] AS VARCHAR(20)),"; }
	if ($coltokens[1] eq "TS" ) { $collist  .="COLUMN$i VARCHAR(78),\n";    $collist2 .="\nCAST(($coltokens[0] (FORMAT '$coltokens[4]'))  AS VARCHAR(26)),"; }
	if ($coltokens[1] eq "DA" ) { $collist  .="COLUMN$i VARCHAR(30),\n";    $collist2 .="\nCAST(($coltokens[0] (FORMAT 'YYYY-MM-DD'))  AS VARCHAR(10)),"; }
	if ($coltokens[1] eq "AT" ) { $collist  .="COLUMN$i VARCHAR(45),\n";    $collist2 .="\nCAST(($coltokens[0] (FORMAT '$coltokens[4]'))  AS VARCHAR(15)),"; }
	if ($coltokens[1] eq "D"  ) { 
					$collist   .="COLUMN$i VARCHAR(". (3 * ($coltokens[7] + 2)). "),\n";     
					$collist2  .="\nCAST($coltokens[0]  AS VARCHAR(".($coltokens[7] + 2).")),"; 
				    }
	if ($coltokens[1] eq "F" )  { $collist  .="COLUMN$i VARCHAR(66),\n";     $collist2 .="\nCAST(($coltokens[0] (FORMAT '-9.99999999999999E-999'))  AS VARCHAR(22)),"; }
	if ($coltokens[1] eq "SZ" ) { $collist  .="COLUMN$i VARCHAR(96),\n";     $collist2 .="\nCAST(($coltokens[0] (FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z'))  AS VARCHAR(32)),"; }
	if ($coltokens[1] eq "TZ" ) { $collist  .="COLUMN$i VARCHAR(63),\n";     $collist2 .="\nCAST(($coltokens[0] (FORMAT 'HH:MI:SS.S(6)Z'))  AS VARCHAR(21)),"; }
	if ($coltokens[1]  ~~ @intervals ) { $collist   .="COLUMN$i VARCHAR(". (3*$coltokens[6]) . "),\n";     $collist2 .="\nCAST($coltokens[0]  AS VARCHAR($coltokens[6])),"; }

}
close(TABLEDDL);

$collist = substr($collist,0,-2);                       #remove last comma + \n  in the columnt list
$collist2 = substr($collist2,0,-1);                     #remove last comma in the column list
$collist3 = substr($collist3,0,-1);                     #remove last comma in the column list

if ($export) {

print "Creting ${schematable}_texport.tpt for export ... \n" if ($debug);

#build the TPTE job spec files for export & import below
open(TPTE,">${schematable}_texport.tpt");
chmod 0600, "${schematable}_texport.tpt";

print TPTE "USING CHAR SET UTF8 DEFINE JOB HDFSEXPORT \n";
print TPTE "DESCRIPTION 'EXPORT TERADATA table with filter into formatted flat file for HDFS archival'\n";
print TPTE "(\nDEFINE SCHEMA schema1\nDESCRIPTION 'table to archive to HDFS '\n";
print TPTE "(\n$collist\n);";

my $qbandinfo;
print TPTE "\n DEFINE OPERATOR DATA_OUT\nDESCRIPTION 'TERADATA PARALLEL TRANSPORTER SQL SELECTOR OPERATOR'\n";
if ($prior_size != 0 &&  $prior_size < $SQL_SELECTOR_MAX ) { 
	# use SQL SELECTOR, small exports 
	print TPTE "TYPE SELECTOR\nSCHEMA schema1\nATTRIBUTES\n(\n";
	$qbandinfo .= "Operator=Selector;";
} else { 
	# use FAST EXPORT, larger exports
	print TPTE "TYPE EXPORT\nSCHEMA schema1\nATTRIBUTES\n(\n";
	$qbandinfo .= "Operator=FastExport;";
	if ($prior_size != 0 &&  $prior_size < $SPOOL_LIMIT) {
        	print TPTE "VARCHAR SpoolMode = 'NoSpool',\n"; #default is spool
		$qbandinfo .= "UseSpool=No;";
	} else {
		print TPTE "VARCHAR SpoolMode = 'Spool',\n"; #default is spool
		$qbandinfo .= "UseSpool=Yes;";
	}
}

print TPTE "VARCHAR QueryBandSessInfo = 'type=Replicator;" . $qbandinfo . "',\n";
print TPTE "VARCHAR PrivateLogName = 'export_${schematable}_log',\n";
print TPTE "VARCHAR TdpId = '$s_tpdid',\n";
print TPTE "VARCHAR UserName = '$s_userid',\n";
print TPTE "VARCHAR UserPassword = '" . `echo -n $s_pass`. "',\n";

        $sql = "LOCK ROW FOR ACCESS SELECT $collist2  FROM $Tschema.$Ttable";
        $sql .= " WHERE $increm_key=${increm_value_original}" if ($increm_key) ;
        $sql =~ s/'/''/g;

print TPTE "VARCHAR SelectStmt = '${sql}'\n";
print TPTE ");\n";

print TPTE "DEFINE OPERATOR FILE_WRITER\n";
print TPTE "DESCRIPTION 'TERADATA PARALLEL TRANSPORTER FLAT FILE WRITER'\n";
print TPTE "TYPE DATACONNECTOR CONSUMER\n";
print TPTE "SCHEMA *\n";
print TPTE "ATTRIBUTES\n(\n";
print TPTE "VARCHAR PrivateLogName = 'dataconnector_${schematable}_log',\n";
print TPTE "VARCHAR DirectoryPath = '.',\n";
print TPTE "VARCHAR FileName = '${schematable}-pipe',\n";
print TPTE "VARCHAR Format = 'Formatted',\n";
print TPTE "VARCHAR Openmode = 'Write',\n";
print TPTE "VARCHAR IndicatorMode = 'Y'\n";
print TPTE ");\n";
print TPTE "APPLY TO OPERATOR (FILE_WRITER)\n";
print TPTE "SELECT * FROM OPERATOR (DATA_OUT);\n";
print TPTE ");\n";
close(TPTE);

#create named pipe
print "Creating named pipe ${schematable}-pipe ... \n" if ($debug);
print `if [ -a ${schematable}-pipe ]; then rm  ${schematable}-pipe; fi; mkfifo ${schematable}-pipe; twbrmcp ${schematable} 1>/dev/null;`;

#run tbuild in background
print "Running tbuild in background for  ${schematable}_texport.tpt -j ${schematable} ... \n" if ($debug);
`tbuild -f ${schematable}_texport.tpt -j ${schematable} > ${schematable}_TPTE.out 2>&1 &`;

my $count =`hadoop fs -ls -R ${namenode}${hdfsfolder}  2>/dev/null| wc -l`;
`hadoop fs -rm -r ${namenode}${hdfsfolder}` if ($count > 0);

(my $hdfsdonefolder = $hdfsfolder) =~ s|${prefix}|${prefix}_done|;
$count =`hadoop fs -ls -R ${namenode}${hdfsdonefolder} | wc -l`;
print `hadoop fs -rm -r ${namenode}${hdfsdonefolder}` if ($count > 0 );

print "Uploading to ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo ....\n" if ($debug);

my $filesize;
if (index($hdfsfolder, "dim") != -1){
	$filesize=140000000;  #dim tables
} else {
	$filesize=8589934592; #fact tables changed the filesize=1073741824
}


my $hive=`cat ${schematable}-pipe | hadoop jar /usr/local/lib/hive/lib/hive-teradata-fast-export-serde-1.0-SNAPSHOT.jar com.groupon.hive.serde.TDFastExportFileSpliter -D td_export.max_file_size=${filesize} /dev/stdin ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo 2>&1 `;

print "Uploading manifest file to ${namenode}${hdfsfolder}/manifest/${hdfsfile}.manifest ....\n" if ($debug);
print `cat ${schematable}.manifest | hadoop fs -put -  ${namenode}${hdfsfolder}/manifest/${hdfsfile}.manifest`;

#print "\n\n\n\n\n";
#print $hive;
#print "\n\n\n\n\n";

my @lines = split /\n/, $hive;
my $lzo_rows=0;
HIVE: foreach my $ln (@lines) {
        if ($ln =~ /total num rows=(\d+)/) {
		$lzo_rows = $1;
                last HIVE;
	}
  }#HIVE
my $manifest =` hadoop fs -text ${namenode}${hdfsfolder}/manifest/${hdfsfile}.manifest  2>/dev/null | head -1 | cut -d "|" -f2 | tr -d " " `;
chomp($manifest);
                if ( $manifest >  $lzo_rows ) {
                        print "EXPORT Status:MISMATCH ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo Manifest ($manifest) <> LZO ($lzo_rows)";
                        `cat ${schematable}_TPTE.out`;
                }
                else {
                        print "EXPORT Status:CORRECT ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo. Manifest  ($manifest) = LZO ($lzo_rows)";
                        #call hive layer
                        my $dt = ( split '/', ${hdfsfolder} )[ -1 ];
                        print `/home/etl_adhoc/Replicator/scripts/backup_to_cerebro_test/create_td_backup_tab.py -vv --table_name=${Tschema}.${Ttable} --backup_dir=${namenode}${prefix} --done_dir=${namenode}${prefix}_done --part=${dt} 2>&1 `;
			#print `/home/etl_adhoc/Replicator/opt_new/logs/create_td_backup_tab.py -vv --table_name=${Tschema}.${Ttable} --part=${dt}`  ;
                         if ($? != 0) {
                                exit(1);
                          }
		}
}# finished export

if ($import) {

print "Creating import TPT file  ${schematable}_timport.tpt ...\n" if($debug);
open(TPTI,">${schematable}_timport.tpt");
chmod 0600, "${schematable}_timport.tpt";

my $inssel;
if ($stagingschema) {
	$sql = "DELETE FROM $importschema.$Ttable WHERE $increm_key=$increm_value_original;";
	$sql =~ s/'/''/g;
	$inssel= "STEP Finalize_Data \n ( \n APPLY ('$sql'),\n('INSERT INTO $importschema.$Ttable SELECT * FROM  $stagingschema.$Ttable ;') TO OPERATOR (DDL_OPERATOR); ); \n";
	
}
else {
	$stagingschema = $importschema;
}

print TPTI "USING CHAR SET UTF8 DEFINE JOB HDFSIMPORT\n";
print TPTI "DESCRIPTION 'IMPORT HDFS file into target TERADATA table '\n";
print TPTI "(\nDEFINE SCHEMA SCHEMA1\nDESCRIPTION 'table to import'\n";
print TPTI "(\n$collist\n);";

print TPTI "\nDEFINE OPERATOR FILE_READER\nDESCRIPTION 'TERADATA PARALLEL TRANSPORTER IMPORT OPERATOR'\nTYPE DATACONNECTOR PRODUCER\nSCHEMA SCHEMA1\nATTRIBUTES\n(\n";
print TPTI "VARCHAR PrivateLogName = 'dataconnector_producer_${schematable}_log',\n";
print TPTI "VARCHAR DirectoryPath = '',\n";
print TPTI "VARCHAR FileName = '${schematable}-ipipe',\n";
print TPTI "VARCHAR Format = 'Formatted',\n";
print TPTI "VARCHAR Openmode = 'Read',\n";
print TPTI "VARCHAR IndicatorMode = 'Y'\n";
print TPTI ");\n";

print TPTI "\n\nDEFINE OPERATOR DDL_OPERATOR\nTYPE DDL\nATTRIBUTES\n(\n";
print TPTI "VARCHAR TdpId = '$t_tpdid',\n";
print TPTI "VARCHAR UserName = '$t_userid',\n";
print TPTI "VARCHAR UserPassword = '".`echo -n $t_pass`. "' ,\n";
print TPTI "VARCHAR ErrorList = '3807'\n";
print TPTI ");\n";

	my $errorttable = substr (${Ttable}, -27) ; # limited to 30 chars by TD, reserve 3 chars for _E1/_E2/_LT

print TPTI "DEFINE OPERATOR FLOAD_OPERATOR\n";
print TPTI "DESCRIPTION 'TERADATA PARALLEL TRANSPORTER LOAD OPERATOR'\n";
print TPTI "TYPE UPDATE\n";
print TPTI "SCHEMA *\n";
print TPTI "ATTRIBUTES\n(\n";
print TPTI "VARCHAR PrivateLogName = 'loadoper_privatelog',\n";
print TPTI "VARCHAR TargetTable = '$stagingschema.$Ttable',\n";
print TPTI "VARCHAR TdpId = '$t_tpdid',\n";
print TPTI "VARCHAR UserName = '$t_userid',\n";
print TPTI "VARCHAR UserPassword = '".`echo -n $t_pass`."',\n";
print TPTI "VARCHAR ErrorTable1 = '$stagingschema.${errorttable}_E1',\n";
print TPTI "VARCHAR ErrorTable2 = '$stagingschema.${errorttable}_E2',\n";
print TPTI "VARCHAR LogTable = '$stagingschema.${errorttable}_LT'\n";
print TPTI ");\n";

print TPTI "STEP Setup_Tables\n(\nAPPLY\n";
print TPTI "('DROP TABLE $stagingschema.${errorttable}_E1;'),\n";
print TPTI "('DROP TABLE $stagingschema.${errorttable}_E2;'),\n";
print TPTI "('DROP TABLE $stagingschema.${errorttable}_WT;'),\n";
print TPTI "('DROP TABLE $stagingschema.${errorttable}_LT;'),\n";
print TPTI "('DROP TABLE $stagingschema.$Ttable;'),\n";
	my $ddl=`tail --lines=+2  $schematable.manifest`;
	$ddl =~ s/'/''/g;
	$ddl =~ s/${Tschema}/$stagingschema/g;
print TPTI "('$ddl')\n";
#DROP hash indexes, they usually have a name
	my $sql = "$header \n select distinct 'DROP INDEX ' || trim(indexname) || ' ON $stagingschema.$Ttable; ' as R1 from dbc.indices where indexType in ('N') and tablename = '$Ttable' and databasename ='${Tschema}';\n $footer";
        my $result = `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;  
print TPTI ",\n('$result')\n" if ($result);

#DROP join index
	 my $sql = "$header \n select distinct 'DROP JOIN INDEX ${stagingschema}.' || trim(indexname) || ';' as R1 from dbc.indices where indexType in ('J') and tablename = '$Ttable' and databasename ='${Tschema}';\n $footer";
        my $result = `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;
print TPTI ",\n('$result')\n" if ($result);

print "Creating drop secondary indexes for table $stagingschema.$Ttable to prepare for MLOAD ...\n" if ($debug);
#drop unique secondary indexes, may not have name, may need to use column list
	my $sql =  "$header \n"
	."select 'DROP INDEX (' || C1"
		." || case WHEN T.C2 is not null then ',' || T.c2 else '' end"
		." || case WHEN T.C3 is not null then ',' || T.c3 else '' end"
		." || case WHEN T.C4 is not null then ',' || T.c4 else '' end"
		." ||') ON $stagingschema.$Ttable;' AS R1 "
		." FROM (select	max(case columnposition when 1 then TRIM(columnname) end) as C1,"
		."		max(case columnposition when 2 then TRIM(columnname) end) as C2,"
		."		max(case columnposition when 3 then TRIM(columnname) end) as C3,"
		."		max(case columnposition when 4 then TRIM(columnname) end) as C4"
		." from dbc.indices where tablename ='${Ttable}' and databasename ='${Tschema}' and indexType='U' ) as T ;\n $footer";
        my $result = `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;

print TPTI ",\n('$result')\n" if ($result);


print TPTI "TO OPERATOR (DDL_OPERATOR);\n";
print TPTI ");\n";

print TPTI "STEP Load_table\n(\nAPPLY\n";
print TPTI "('INSERT INTO $stagingschema.$Ttable($collist3);')\n";
print TPTI "TO OPERATOR (FLOAD_OPERATOR[2])\n";
print TPTI "\nSELECT * FROM OPERATOR(FILE_READER[2]);\n";
print TPTI ");\n";
print TPTI "$inssel\n";
print TPTI ");\n";
close(TPTI);

	#get manifest file + rowcount
	print "Get manifest file ...\n" if ($debug);
	my $manifestrowcount =` hadoop fs -text ${namenode}${hdfsfolder}/manifest/${hdfsfile}.manifest  2>${schematable}.manifest.err | head -1 | cut -d "|" -f2 | tr -d " " `;
        chomp($manifestrowcount);
        if ($manifestrowcount eq "" ) {
			print "Invalid manifest file. No import on ${schematable} performed. Exiting ...\n";
			print `cat ${schematable}.manifest.err`;
			cleanup($schematable) if(!$debug);
			exit;
	}

	#create named pipe
	print `if [ -a ${schematable}-ipipe ]; then rm ${schematable}-ipipe ; fi; mkfifo ${schematable}-ipipe`;

	#here we need to get the file and import it
	print "Get LZO file ${namenode}${hdfsfolder}/lzo/*${hdfsfile}.lzo and import it in the background ...\n" if ($debug);
        system("hadoop fs -text ${namenode}${hdfsfolder}/lzo/*${hdfsfile}.lzo   > ${schematable}-ipipe  2>${schematable}-err & ");

	#run tbuild, there is good chance for failure, check
	print "run tbuild TPT ${schematable}_timport.tpt ...\n" if ($debug);
	`twbrmcp ${schematable} 1>/dev/null; tbuild -f ${schematable}_timport.tpt -j ${schematable} > ${schematable}_TPTI.out 2>&1 `;
	
	my $sql = "select CAST(count(*) AS BIGINT) from $importschema.$Ttable";	
	$sql .= " WHERE $increm_key=${increm_value_original}" if ($increm_key) ;

   	my $rowcount=`echo -e " .LOGON $target  \n $sql ; \n.QUIT" | bteq -c UTF8 | grep -v '\\*\\*\\*\\|\\+---' | tail -3 | head -1 | sed 's/\\s*//'`;
        chomp($rowcount);
	
	my $status;
	if ( $manifestrowcount == $rowcount && $rowcount > 0 ) {
			$status="CORRECT";
	} else {
			$status="MISMATCH";
			print `cat ${schematable}_TPTI.out `;
			print `cat ${schematable}-err`;
	}
		
        print "IMPORT Status:$status, ${namenode}${hdfsfolder}/lzo/*${hdfsfile}.lzo DB:($rowcount), MANIFEST: ($manifestrowcount)";

} #finish import

#remove local host artifacts
cleanup($schematable) if(!$debug);

chomp($dt=`date '+%Y%m%d%H%M%S'`);
print "|$dt\n";


END {
        if ($import) {        
		#kill pipe writing is still running here
                my $id=`ps -ef | grep 'hadoop fs -text ${namenode}${hdfsfolder}/lzo/\\*${hdfsfile}.lzo' | awk '{print "kill "\$2";"'}`;
                if ($id) {
			print `echo "$id" |  bash`  if ($debug);
                	print "Killed background process id = $id " if ($debug);
		}
        };
	if ($export) {
		my $job;
	        #cleanup section here for background processes
        	print "kill export tbuild process" if ($debug);
		for (my $count = 1; $count <= 10; $count++) {
			$job=`twbstat | grep ${schematable}`;
 			if ($job) {
				print "sleeping 20 seconds....\n" if ($debug);
				sleep(20);
			}
		 }

                print `twbkill $job` if ($job);
	}
};
close($TEE);

########## FUNCTIONS DEFINITION #########
sub cleanup() {
my $schematable = $_[0];
		unlink("${schematable}.manifest") if (-e "${schematable}.manifest");
                unlink("${schematable}_texport.tpt") if (-e "${schematable}_texport.tpt");
                unlink("${schematable}.columns") if (-e "${schematable}.columns");
		unlink("${schematable}-pipe") if (-e "${schematable}-pipe");
		unlink("${schematable}-ipipe") if (-e "${schematable}-ipipe");
		unlink("${schematable}_timport.tpt") if (-e "${schematable}_timport.tpt");

		`rm ${schematable}* `;

}

sub usage()
    {
     print STDERR << "EOF";
     $_[0] hdfs_backup.pl exports and archives to HDFS Teradata tables. Supports full and incremental.
      Usage: $0


     --source  	: host/userid,password for Teradata export cluster in this format. Usually PROD env. Example --source tdwa/user,password
     --target   : host/userid,password for Teradata import cluster in this format. Example --target tdwc/user,password
     --table 	: no space allowed. Either (1) databasename,tablename for full table or (2) databasename,tablename,key,value for incremental
     --export 	: export table to HDFS only. Can be used with import.
     --import	: import table to HDFS only. Can be used with export.
     --importschema	: import table in this database, drop importschema.table if preexisting. Use stagingschema for incremental
     --stagingschema	: For incremenally loaded tables, create and load stagingschema.table and run an insert select into final importschema.table. Removes existing records for key=value from import schema 
			  to avoid duplicate insertions for multiple runs.
     --date	: Optional for full table exports/imports only. If specified forces export / import to ${date} folder in HDFS. If not specified currentday is used.
     --prefix   : hdfs folder location
     --help 	: this (help) message
     --debug    : persist intermediate files for debugging, prints more verbose status messages

	Examples:

	Export
	======
	FULL : ./hdfs_exim.pl --export --source tdwa/datamover01_dba,... --table sandbox,campaigns_narrow --prefix /td_backup  
	INCR.: ./hdfs_exim.pl --export --source tdwa/datamover01_dba,... --table prod_groupondw,fact_email_sends,send_date_key,20130723 --prefix /td_backup 

	Import
	=====
	FULL : ./hdfs_exim.pl --import --source tdwa/datamover01_dba,.... --target tdwc/datamover01_dba,.... --table  sandbox,campaigns_narrow --prefix /td_backup --importschema prod_groupondw  
	INCR.: ./hdfs_exim.pl --import --source tdwa/datamover01_dba,.... --target tdwc/datamover01_dba,.... --table  prod_groupondw,fact_email_sends,send_date_key,20130723  --prefix /td_backup --importschema prod_groupondw --stagingschema staging
 
	Notes:1.  Source parameter is still required in an import to extract some metadata info ( index, column types) from source cluster. as such source table must exist on source cluster. 
	      2.  If password contains reserved character ex. $, escape it twice : \\\$

EOF
        exit;
}
