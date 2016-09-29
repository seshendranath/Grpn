#!/usr/local/bin/perl

use strict;
#use warnings;
use Getopt::Long;
use Time::Local;
use Cwd 'abs_path';
use File::Basename;


#define variables
my ($source,$table,$export,$help,$rc,$debug,$target,$import,$importschema,$stagingschema,$date,$namenode,$inssel,$errorttable,$manifestrowcount,$exclude,$writers,$TEE,$result,$sql,$fastexporters,$count,$removedelimiters,$encoding);
my $SPOOL_LIMIT = 22000 ; #in MB LZO compressed
my $SQL_SELECTOR_MAX = 1000 ; #in MB LZO compressed
my $prefix = "/user/grp_gdoop_edw_rep_prod";

#process comand line variables
GetOptions(
    'source=s' => \$source,
    'target=s' => \$target,
    'table=s'  => \$table,
    'export'   => \$export,
    'import'   => \$import,
    'importschema=s' => \$importschema,
    'stagingschema=s' => \$stagingschema,
    'date=s'   => \$date,
    'prefix=s' => \$prefix,
    'help'     => \$help,
    'debug'    => \$debug,
    'namenode=s' => \$namenode,
    'exclude=s'  => \$exclude,
    'writers=s' => \$writers,
    'fastexporters=s' => \$fastexporters,
    'removedelimiters' => \$removedelimiters,
    'encoding=s'  => \$encoding
) or usage();
usage() if ($help || !$table || ($export && $import) );

#capture start time for computing elapsed time 
my $startt = time;

$0="$0  --table $table"; #remove password from command line visible in ps bash command
my($filename, $dir,$suffix) = fileparse(abs_path($0));

my ($t_tpdid,$t_userid,$t_pass);
if ($target) {
	$target =~ /(.*)\/(.*),(.*)/ or usage("====>invalid target connect string $target \n ");
	$t_tpdid  = $1;
	$t_userid = $2;
	$t_pass   = $3;
	$0 = "$0 --target $t_tpdid/$t_userid";
} 

$namenode="" if (!$namenode); #supposed to run this script from a hadoop client node hdfs://gdoop-namenode-vip.snc1:8020

$encoding="UTF8" if (!$encoding); # initialize encoding since most of the time not used
my $multiplier=($encoding eq "UTF8")?3:2; # used for VARCHAR/CHAR byte calculation, UTF8 uses 3 bytes , UTF16 uses 2 bytes / character

#use 5 threads if writers not specified
$writers="5" if (!$writers);

#use 1 exporter if not specified
$fastexporters ="1" if (!$fastexporters);


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
	my $schematable = "${Tschema}.${Ttable}.${increm_value}.${currentday}"; 		
	$schematable =~ s/[-]//g; #remove invalid - character from variable used as filename 

	#log file location to exportlogs/${schematable}.log
	my $logfile="$ENV{HOME}/exportlogs/${schematable}.log";    #log file location
	unless(-e "$ENV{HOME}/exportlogs" or mkdir "$ENV{HOME}/exportlogs" ) { die "Unable to create logs\n" } ;  #create logs folder if it does not exist

	open $TEE, '|-', "tee ${logfile}"  or die "Can't redirect STDOUT: $!";
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
	(my $hdfsfolder_tmp = $hdfsfolder) =~ s/${prefix}/\/tmp/;

	#BTEQ header / footer
        my $st_var;
        if ($export) {
              $st_var = $source;
        } elsif ($import) {
              $st_var = $target;
        } else {
		print "Need to specify either export or import \n Exiting with error\n";
        	exit;
	}       
	my $header=".SET RECORDMODE OFF\n.SET WIDTH 5000\n.set separator '|'\n.set TITLEDASHES OFF\n.set ECHOREQ OFF\n.LOGON ".$st_var."\n";
        my $footer=".EXPORT RESET;\n.QUIT";
	#used to calculate size of a previous export in hadoop to assess when to use spool / no spool for export
	my ($prior_list, $prior_size);
 
 if ($export) {

	$prior_list=`hadoop fs -ls ${namenode}${prefix}/${Tschema}.${Ttable} 2>/dev/null|tail -5|awk '{print \$8}'`;
        $prior_list =~ s/\n/ /g;

        $prior_size = 0 ;
        if ($prior_list) {
                $prior_size=`hadoop fs -du -s ${prior_list} |awk 'max=(max>\$1)?max:\$1; END {print max/1024/1024}' | tail -1`;
        }
        print "Prior_size(Mib) $prior_size\n";

	#create manifest file on current node
	unlink("$ENV{HOME}/exportlogs/$schematable.manifest") if (-e "$ENV{HOME}/exportlogs/$schematable.manifest");
	my $where = " WHERE $increm_key=${increm_value_original}" if ($increm_key) ;
	my $sql = ".EXPORT DATA FILE = $ENV{HOME}/exportlogs/$schematable.manifest \n $header ";
	$sql   .= "\nLOCK row for access select current_timestamp,CAST(count(*) AS BIGINT) from ${Tschema}.${Ttable} $where; ";
	$sql   .= "\nSHOW TABLE ${Tschema}.${Ttable};\n $footer";

	print "Creating manifest file $ENV{HOME}/exportlogs/$schematable.manifest ....\n";
	`echo "$sql" | bteq -c UTF8 `  or die "Failure to get manifest for ${Tschema}.${Ttable} : $!";
	#check for missing table and abort work , cleanup
	$rc=$? >> 8;
	exit if ($rc);

 } elsif ($import) {
 	#get manifest file + rowcount
        print "Get manifest file ...\n";
	`hadoop fs -text ${namenode}${hdfsfolder}/manifest/${hdfsfile}.manifest >$ENV{HOME}/exportlogs/${schematable}.manifest  2>$ENV{HOME}/exportlogs/${schematable}.manifest.err`;
        $manifestrowcount =`head -1 $ENV{HOME}/exportlogs/${schematable}.manifest | cut -d "|" -f2 | tr -d " " `;

        chomp($manifestrowcount);
        if ($manifestrowcount eq "" ) {
                        print "Invalid manifest file. No import on ${schematable} performed. Exiting ...\n";
                        print `cat $ENV{HOME}/exportlogs/${schematable}.manifest.err`;
                        cleanup($schematable) if(!$debug);
                        exit;
 	}

	#now that we have the manifest file, let's extract DDL and execute it
	#set stagingschema if it is being used
	if ($stagingschema) {
           my $sql = "DELETE FROM $importschema.$Ttable WHERE $increm_key=$increm_value_original;";
              $sql =~ s/'/''/g;
              $inssel= "STEP Finalize_Data\n(\n APPLY ('$sql'),\n('INSERT INTO $importschema.$Ttable SELECT * FROM  $stagingschema.$Ttable ;') TO OPERATOR(DDL_OPERATOR););\n";
	}
	else {
        	$stagingschema = $importschema;
	}
	$errorttable = substr (${Ttable}, -27) ; # limited to 30 chars by TD, reserve 3 chars for _E1/_E2/_LT

   	my $sql;
        my @suffix_errors = ("${errorttable}_E1","${errorttable}_E2","${errorttable}_WT","${errorttable}_LT",${Ttable});
        foreach (@suffix_errors)
        {
                $sql .= "select 1 from dbc.TablesV where databasename = '$stagingschema' and TableName = '$_';\n";
                $sql .= ".if activitycount = 1 then DROP TABLE ${stagingschema}.$_;\n";
        }
                my $ddl=`tail --lines=+2  $ENV{HOME}/exportlogs/$schematable.manifest`;
                   $ddl =~ s/TABLE ${Tschema}\./TABLE $stagingschema\./g;
           $sql .= $ddl;

	`echo -e "$header $sql $footer" | bteq -c UTF8 `  or die "Failure to create DDL for  ${stagingschema}.${Ttable} : $!";
	#check for missing table and abort work , cleanup
        $rc=$? >> 8;
        exit if ($rc);
 }

	print "Creating columns file $ENV{HOME}/exportlogs/$schematable.columns ... \n"; 
	unlink("$ENV{HOME}/exportlogs/$schematable.columns") if (-e "$ENV{HOME}/exportlogs/$schematable.columns");
	if ($export) {
		my $sql = ".EXPORT DATA FILE = $ENV{HOME}/exportlogs/$schematable.columns \n $header HELP TABLE ${Tschema}.${Ttable};\n $footer";
		`echo "$sql" | bteq -c UTF8 `  or die "Failure to get columns for ${Tschema}.${Ttable} : $!";
	} elsif ($import)  {
		my $sql = ".EXPORT DATA FILE = $ENV{HOME}/exportlogs/$schematable.columns \n $header HELP TABLE ${stagingschema}.${Ttable};\n $footer";
		`echo "$sql" | bteq -c UTF8 `  or die "Failure to get columns for ${Tschema}.${Ttable} : $!";
	}

	#creating column lists
	#collist   => used for TPT schema description export/import
	#collist2  => used for TPT select statement export
	#collist3  => used for TPT operator import
	
	my @exclude_columns = split(/\,/,$exclude) if ($exclude);      #exclude columns

	open(TABLE_COLS,"$ENV{HOME}/exportlogs/$schematable.columns");
	my ($collist,$collist2,$collist3) = "";
	my $i = 0;
	my $len;
	my @intervals = qw(DH DM DS DY HM HR HS MI MO MS SC YM YR);
	
	while(<TABLE_COLS>) {    
	s/\s*|,//g; #eliminate spaces and commas from column file as preparation for parsing below
	my @coltokens = split(/\|/,$_);

	$i++;
	$collist3 .=":COLUMN$i,";

	if (grep {$_ eq $coltokens[0]}  @exclude_columns) { 
		 #excluded 
		 $collist   .="COLUMN$i VARCHAR(12),\n";
                 $collist2  .="\nCAST('\\N' as VARCHAR(4)),";
	} 
	else 	{ #non-excluded col
		$coltokens[0] = "\"$coltokens[0]\""; # need to enclose in doublequotes to avoid increm_keyword collisions in TPT
		if ($coltokens[1] eq "CV" || $coltokens[1] eq "CF") 
		 {		$len = $coltokens[4];
				$len =~ s/[X()]+//g; #extract number for x(ddd) column format
				$len = 2 if ($len == 1);  # because NULL can be 2 characters 
				$collist  .="COLUMN$i VARCHAR(".($multiplier * $len)."),\n"; 

				if ($removedelimiters) {
					if ($coltokens[14] eq "1") { # LATIN column
						#$collist2 .="\nCAST(TRANSLATE(TD_SYSFNLIB.OREPLACE(TD_SYSFNLIB.OREPLACE(COALESCE($coltokens[0],'\\N'),'0a'xc,''),'01'xc,'') USING UNICODE_TO_LATIN)  AS VARCHAR(".$len.")),";
						$collist2 .="\nCAST(SYSLIB.OTRANSLATE(COALESCE($coltokens[0],'\\N'),'0a'xc,'') AS VARCHAR(".$len.")),"; #check bug in LAT=> UNI=> LATIN
					} else { #UNICODE
						$collist2 .="\nCAST(TD_SYSFNLIB.OREPLACE(COALESCE($coltokens[0],'\\N'),'0a'xc,'') AS VARCHAR(".$len.")),";
					}
				} else {
					$collist2 .="\nCAST(COALESCE($coltokens[0],'\\N') AS VARCHAR(".$len.")),";
				}


		}
		if ($coltokens[1] eq "BF" || $coltokens[1] eq "BV"  ) {
				$len = $coltokens[4];
                                $len =~ s/[X()]+//g; #extract number for x(ddd) column format
                		# do not include in export, they are not supported
				#$collist  .="COLUMN$i BYTE(".($len)."),\n"; 
				#$collist2 .="\n$coltokens[0],";	
		} 
		if ($coltokens[1] eq "I" )  { $collist  .="COLUMN$i VARCHAR(".($multiplier * 11)."),\n";	$collist2 .="\nCOALESCE(CAST($coltokens[0] AS VARCHAR(11)),'\\N'),"; } 
		if ($coltokens[1] eq "I1" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 4)."),\n";    $collist2 .="\nCOALESCE(CAST($coltokens[0] AS VARCHAR(4)),'\\N'),"; }
		if ($coltokens[1] eq "I2" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 5)."),\n";    $collist2 .="\nCOALESCE(CAST($coltokens[0] AS VARCHAR(5)),'\\N'),"; }
		if ($coltokens[1] eq "I8" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 20)."),\n";    $collist2 .="\nCOALESCE(CAST($coltokens[0] AS VARCHAR(20)),'\\N'),"; }
		if ($coltokens[1] eq "TS" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 26)."),\n";    $collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT '$coltokens[4]'))  AS VARCHAR(26)),'\\N'),"; }
		if ($coltokens[1] eq "DA" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 10)."),\n";    $collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT 'YYYY-MM-DD'))  AS VARCHAR(10)),'\\N'),"; }
		if ($coltokens[1] eq "AT" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 15)."),\n";    $collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT '$coltokens[4]'))  AS VARCHAR(15)),'\\N'),"; }
		if ($coltokens[1] eq "D"  ) { 
						$collist   .="COLUMN$i VARCHAR(". ($multiplier * ($coltokens[7] + 2)). "),\n";     
						$collist2  .="\nCOALESCE(CAST($coltokens[0]  AS VARCHAR(".($coltokens[7] + 2).")),'\\N'),"; 
					    }
		if ($coltokens[1] eq "F" )  { $collist.="COLUMN$i VARCHAR(".($multiplier * 25)."),\n";  $collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT '----,---,---,---,--9.9999'))  AS VARCHAR(25)),'\\N'),"; }
		if ($coltokens[1] eq "SZ" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 32)."),\n";$collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT 'YYYY-MM-DDBHH:MI:SS.S(F)Z'))  AS VARCHAR(32)),'\\N'),"; }
		if ($coltokens[1] eq "TZ" ) { $collist  .="COLUMN$i VARCHAR(".($multiplier * 21)."),\n";$collist2 .="\nCOALESCE(CAST(($coltokens[0] (FORMAT 'HH:MI:SS.S(6)Z'))  AS VARCHAR(21)),'\\N'),"; }
		if ($coltokens[1]  ~~ @intervals ) { $collist.="COLUMN$i VARCHAR(". ($multiplier * $coltokens[6]) . "),\n"; $collist2 .="\nCOALESCE(CAST($coltokens[0]  AS VARCHAR($coltokens[6])),'\\N'),"; }
	} #non excluded col

	} # end while
close(TABLE_COLS);

$collist = substr($collist,0,-2);                       #remove last comma + \n  in the columnt list
$collist2 = substr($collist2,0,-1);                     #remove last comma in the column list
$collist3 = substr($collist3,0,-1);                     #remove last comma in the column list

if ($export) {

#build the TPTE job spec files for export & import below
print "Creating $ENV{HOME}/exportlogs/${schematable}_texport.tpt for export ... \n"; 

open(TPTE,">$ENV{HOME}/exportlogs/${schematable}_texport.tpt");
chmod 0600, "$ENV{HOME}/exportlogs/${schematable}_texport.tpt";

print TPTE "USING CHAR SET $encoding DEFINE JOB HDFSEXPORT \n";
print TPTE "DESCRIPTION 'EXPORT TERADATA table with filter into formatted flat file for HDFS archival'\n";
print TPTE "(\nDEFINE SCHEMA schema1\nDESCRIPTION 'table to archive to HDFS '\n";
print TPTE "(\n$collist\n);";

my $qbandinfo;
print TPTE "\n DEFINE OPERATOR DATA_OUT\nDESCRIPTION 'TERADATA PARALLEL TRANSPORTER SQL SELECTOR OPERATOR'\n";
if ($prior_size != 0 &&  $prior_size < $SQL_SELECTOR_MAX ) {
        # use SQL SELECTOR, small exports
        print TPTE "TYPE SELECTOR\nSCHEMA schema1\nATTRIBUTES\n(\n";
        $qbandinfo .= "Operator=Selector;";
	$fastexporters=1; #can only use 1 exporter  for SQL SELECTOR, more for FAST EXPORT only
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

$qbandinfo .= "Exporters=$fastexporters;Writers=$writers;";
$qbandinfo .= "UtilityDataSize=large;" if ($fastexporters>1);

print TPTE "VARCHAR QueryBandSessInfo = 'type=Replicator;" . $qbandinfo . "',\n";
print TPTE "VARCHAR TdpId = '$s_tpdid',\n";
print TPTE "VARCHAR UserName = '$s_userid',\n";
print TPTE "VARCHAR UserPassword = '" . `echo -n $s_pass`. "',\n";

#print TPTE "INTEGER MaxSessions=100,\n";
#print TPTE "INTEGER MinSessions=100,\n";

my $sql = "LOCK ROW FOR ACCESS SELECT $collist2  FROM $Tschema.$Ttable";
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
print TPTE "VARCHAR DirectoryPath = '$ENV{HOME}/exportlogs',\n";
if ($writers== 1 ) {
	print TPTE "VARCHAR FileName = '${schematable}-pipe-1',\n";
} else {
	print TPTE "VARCHAR FileName = '${schematable}-pipe',\n";
}
print TPTE "VARCHAR Format = 'Delimited',\n";

my $delim=($encoding eq "UTF16")?"0100":"01";
print TPTE "VARCHAR TextDelimiterHex = '${delim}',\n";
print TPTE "VARCHAR NullColumns = 'N',\n";
print TPTE "VARCHAR EscapeTextDelimiter = '\\'\n";

#print TPTE "VARCHAR OpenMode = 'Write'\n";
print TPTE ");\n";
print TPTE "APPLY TO OPERATOR (FILE_WRITER[$writers])\n";
print TPTE "SELECT * FROM OPERATOR (DATA_OUT[$fastexporters]);\n";
print TPTE ");\n";
close(TPTE);

#create named pipe
print "Creating named pipe $ENV{HOME}/exportlogs/${schematable}-pipe-{1..$writers} ... \n";

print `for i in {1..$writers}; do if [ -a $ENV{HOME}/exportlogs/${schematable}-pipe-\${i} ]; then rm  $ENV{HOME}/exportlogs/${schematable}-pipe-\${i}; fi; mkfifo $ENV{HOME}/exportlogs/${schematable}-pipe-\${i}; done\n`;
print `twbrmcp ${schematable} 1>/dev/null;`;

#run tbuild in background
print "Running tbuild for  $ENV{HOME}/exportlogs/${schematable}_texport.tpt -j ${schematable} ... \n";
`tbuild -f $ENV{HOME}/exportlogs/${schematable}_texport.tpt -j ${schematable} > $ENV{HOME}/exportlogs/${schematable}_TPTE.out 2>&1 &`;

(my $hdfsdonefolder = $hdfsfolder) =~ s|${prefix}|${prefix}/done|;
$count =`hadoop fs -ls -R ${namenode}${hdfsdonefolder} | wc -l`;
print `hadoop fs -rm -r ${namenode}${hdfsdonefolder}` if ($count > 0 );

#removing /tmp location
$count =`hadoop fs -ls -R ${namenode}${hdfsfolder_tmp} | wc -l`;
print `hadoop fs -rm -r ${namenode}${hdfsfolder_tmp}` if ($count > 0 );
print "Uploading to ${namenode}${hdfsfolder_tmp}/lzo/${hdfsfile}.lzo ....\n";

#upload into /tmp and don't modify original
my $encoding_conversion=($encoding eq "UTF16")?" | iconv -f UTF-16 -t UTF-8":""; #need to convert back to UTF-8 if exported in UTF-16

print `for x in {1..$writers} ; do cat $ENV{HOME}/exportlogs/${schematable}-pipe-\${x} $encoding_conversion | lzop | hadoop fs -put -  ${namenode}${hdfsfolder_tmp}/lzo/${hdfsfile}-\${x}.lzo 2>&1  & done ; wait`;

print "Uploading manifest file to ${namenode}${hdfsfolder_tmp}/manifest/${hdfsfile}.manifest ....\n";
print `cat $ENV{HOME}/exportlogs/${schematable}.manifest | hadoop fs -put -  ${namenode}${hdfsfolder_tmp}/manifest/${hdfsfile}.manifest`;

print "Moving ${namenode}${hdfsfolder_tmp}/* => ${namenode}${hdfsfolder}\n";

#$count =`hadoop fs -ls -R ${namenode}${hdfsfolder}/* | wc -l`;
print `hadoop fs -rm -r ${namenode}${hdfsfolder};hadoop fs -mkdir -p ${namenode}${hdfsfolder}`;

print `hadoop fs -mv ${namenode}${hdfsfolder_tmp}/* ${namenode}${hdfsfolder} 2>&1`;  #move tmp folder to final location

#add lzo indexer asynchrounously 
print `hadoop -Dmapreduce.job.queuename=public jar /usr/hdp/current/share/lzo/0.6.0/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.DistributedLzoIndexer ${namenode}${hdfsfolder}/lzo/ 1>/dev/null &`;

my $manifest =`cat $ENV{HOME}/exportlogs/${schematable}.manifest  2>/dev/null | head -1 | cut -d "|" -f2 | tr -d " " `;
chomp($manifest);

my $lzo = `grep '^DATA_OUT: Total Rows Exported:' $ENV{HOME}/exportlogs/${schematable}_TPTE.out | cut -d" " -f6`;
chomp($lzo);

if ( $manifest >   $lzo ) {
                        print "EXPORT Status:MISMATCH ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo Manifest($manifest) <> LZO($lzo)\n";
			`cat $ENV{HOME}/exportlogs/${schematable}_TPTE.out`;
} else {
			print "EXPORT Status:CORRECT ${namenode}${hdfsfolder}/lzo/${hdfsfile}.lzo. Manifest($manifest) = LZO($lzo)\n";
			my $dt_p = ( split '/', ${hdfsfolder} )[ -1 ];
			print `${dir}/create_hive_table.py  --hive_table=${Tschema}.${Ttable} \\
					--backup_dir ${namenode}${prefix} \\
                        		--done_dir ${namenode}${prefix}/done \\
					--table_name=${Tschema}.${Ttable} \\
					--part=${dt_p} -vv 2>&1`  ;
		}	

}# finished export

if ($import) {

print "Creating import TPT file  $ENV{HOME}/exportlogs/${schematable}_timport.tpt ...\n";
open(TPTI,">$ENV{HOME}/exportlogs/${schematable}_timport.tpt");
chmod 0600, "$ENV{HOME}/exportlogs/${schematable}_timport.tpt";

print TPTI "USING CHAR SET $encoding  DEFINE JOB HDFSIMPORT\n";
print TPTI "DESCRIPTION 'IMPORT HDFS file into target TERADATA table '\n";
print TPTI "(\nDEFINE SCHEMA SCHEMA1\nDESCRIPTION 'table to import'\n";
print TPTI "(\n$collist\n);";

print TPTI "\nDEFINE OPERATOR FILE_READER\nDESCRIPTION 'TERADATA PARALLEL TRANSPORTER IMPORT OPERATOR'\nTYPE DATACONNECTOR PRODUCER\nSCHEMA SCHEMA1\nATTRIBUTES\n(\n";
print TPTI "VARCHAR PrivateLogName = 'dataconnector_producer_${schematable}_log',\n";
print TPTI "VARCHAR DirectoryPath = '$ENV{HOME}/exportlogs',\n";
print TPTI "VARCHAR FileName = '${schematable}-ipipe',\n";
print TPTI "VARCHAR Format = 'Delimited',\n";
print TPTI "VARCHAR TextDelimiterHex = '01'\n";
#print TPTI "VARCHAR EscapeTextDelimiter = '\\'\n";
print TPTI ");\n";

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

print "Creating drop secondary indexes for table $stagingschema.$Ttable to prepare for MLOAD ...\n";
#DROP hash indexes, they usually have a name
	$sql = "$header \n select distinct 'DROP INDEX ' || trim(indexname) || ' ON $stagingschema.$Ttable; ' as R1 from dbc.indices where indexType in ('N') and tablename = '$Ttable' and databasename ='${Tschema}';\n $footer";
        $result .= `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;  
#DROP join index
	$sql = "$header \n select distinct 'DROP JOIN INDEX ${stagingschema}.' || trim(indexname) || ';' as R1 from dbc.indices where indexType in ('J') and tablename = '$Ttable' and databasename ='${Tschema}';\n $footer";
        $result .= `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;

#DROP unique secondary indexes USI, may not have name, may need to use column list
	$sql =  "$header \n"
	."select 'DROP INDEX (' || C1"
		." || case WHEN T.C2 is not null then ',' || T.c2 else '' end"
		." || case WHEN T.C3 is not null then ',' || T.c3 else '' end"
		." || case WHEN T.C4 is not null then ',' || T.c4 else '' end"
		." ||') ON $stagingschema.$Ttable;' AS R1 "
		." FROM (select	max(case columnposition when 1 then TRIM(columnname) end) as C1,"
		."		max(case columnposition when 2 then TRIM(columnname) end) as C2,"
		."		max(case columnposition when 3 then TRIM(columnname) end) as C3,"
		."		max(case columnposition when 4 then TRIM(columnname) end) as C4"
		." from dbc.indices where tablename ='${Ttable}' and databasename ='${Tschema}' and indexType IN ('U','K') ) as T ;\n $footer"; # check if S  or V required here ?!?!
        $result .= `echo -e "$sql" | bteq -c UTF8 | grep DROP | tail -1 `;

print TPTI "STEP Load_table\n(\nAPPLY\n";
print TPTI "('INSERT INTO $stagingschema.$Ttable($collist3);')\n";
print TPTI "TO OPERATOR (FLOAD_OPERATOR[2])\n";
print TPTI "\nSELECT * FROM OPERATOR(FILE_READER[2]);\n";
print TPTI ");\n";
print TPTI "$inssel\n";
print TPTI ");\n";
close(TPTI);

	#create named pipe
	print `if [ -a $ENV{HOME}/exportlogs/${schematable}-ipipe ]; then rm $ENV{HOME}/exportlogs/${schematable}-ipipe ; fi; mkfifo $ENV{HOME}/exportlogs/${schematable}-ipipe`;

	#here we need to get the file and import it
	print "Get LZO file ${namenode}${hdfsfolder}/lzo/${hdfsfile}-*.lzo and import it in the background ...\n";
        system("hadoop fs -text ${namenode}${hdfsfolder}/lzo/${hdfsfile}-*.lzo   > $ENV{HOME}/exportlogs/${schematable}-ipipe  2>$ENV{HOME}/exportlogs/${schematable}-err & ");

	#run tbuild, there is good chance for failure, check
	print "run tbuild TPT $ENV{HOME}/exportlogs/${schematable}_timport.tpt ...\n";
	`twbrmcp ${schematable} 1>/dev/null; tbuild -f $ENV{HOME}/exportlogs/${schematable}_timport.tpt -j ${schematable} > $ENV{HOME}/exportlogs/${schematable}_TPTI.out 2>&1 `;
	
	my $sql = "select CAST(count(*) AS BIGINT) from $importschema.$Ttable";	
	$sql .= " WHERE $increm_key=${increm_value_original}" if ($increm_key) ;

   	my $rowcount=`echo -e " .LOGON $target  \n $sql ; \n.QUIT" | bteq -c UTF8 | grep -v '\\*\\*\\*\\|\\+---' | tail -3 | head -1 | sed 's/\\s*//'`;
        chomp($rowcount);
	
	my $status;
	if ( $manifestrowcount == $rowcount && $rowcount > 0 ) {
			$status="CORRECT";
	} else {
			$status="MISMATCH";
			print `cat $ENV{HOME}/exportlogs/${schematable}_TPTI.out `;
			print `cat $ENV{HOME}/exportlogs/${schematable}-err`;
	}
		
        print "IMPORT Status:$status, ${namenode}${hdfsfolder}/lzo/*${hdfsfile}.lzo DB:($rowcount), MANIFEST: ($manifestrowcount)";

} #finish import

#remove local host artifacts
cleanup($schematable) if(!$debug);

my $elapsed = time - $startt;
print "Elapsed time: $elapsed secs\n";

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
		if (${schematable}) {
        		print "kill export tbuild process \n" if ($debug);
			for (my $count = 1; $count <= 10; $count++) {
			$job=`twbstat | grep ${schematable}`;
 			if ($job) {
				print "sleeping 20 seconds....\n" if ($debug);
				sleep(20);
				}
		 	}#end for

                print `twbkill $job` if ($job);
		}#end if 
	}
};
close($TEE);

########## FUNCTIONS DEFINITION #########
sub cleanup {
my $schematable = $_[0];
                unlink("$ENV{HOME}/exportlogs/${schematable}.manifest") if (-e "$ENV{HOME}/exportlogs/${schematable}.manifest");
                unlink("$ENV{HOME}/exportlogs/${schematable}_texport.tpt") if (-e "$ENV{HOME}/exportlogs/${schematable}_texport.tpt");
                unlink("$ENV{HOME}/exportlogs/${schematable}.columns") if (-e "$ENV{HOME}/exportlogs/${schematable}.columns");
                for (my $i=1; $i <= $writers; $i++) {
                         unlink("$ENV{HOME}/exportlogs/${schematable}-pipe-${i}") if (-e "$ENV{HOME}/exportlogs/${schematable}-pipe-${i}");
                }
                unlink("$ENV{HOME}/exportlogs/${schematable}-ipipe") if (-e "$ENV{HOME}/exportlogs/${schematable}-ipipe");
                unlink("$ENV{HOME}/exportlogs/${schematable}_timport.tpt") if (-e "$ENV{HOME}/exportlogs/${schematable}_timport.tpt");
                `find $ENV{HOME}/exportlogs/${schematable}* ! -name *log -delete `;
                #`rm $ENV{HOME}/exportlogs/${schematable}* `;

}
sub usage {
     print STDERR << "EOF";
     $_[0] hdfs_backup.pl exports and archives to HDFS Teradata tables. Supports full and incremental.
      Usage: $0
     --source   : host/userid,password for Teradata export cluster in this format. Usually PROD env. Example --source tdwa/user,password
     --target   : host/userid,password for Teradata import cluster in this format. Example --target tdwc/user,password
     --table    : no space allowed. Either (1) databasename,tablename for full table or (2) databasename,tablename,key,value for incremental
     --export   : export table to HDFS only. Can be used with import.
     --import   : import table to HDFS only. Can be used with export.
     --importschema     : import table in this database, drop importschema.table if preexisting. Use stagingschema for incremental
     --stagingschema    : For incrementally loaded tables, create and load stagingschema.table and run an insert select into final importschema.table.
                                Removes existing records for key=value from import schema   to avoid duplicate insertions for multiple runs.
     --date     : Optional for full table exports/imports only. If specified forces export / import to ${date} folder in HDFS. i
                        If not specified currentday is used.
     --prefix   : hdfs folder location
     --exclude  : comma separated  list of columns to avoid from export col1,col2,col3 Note:Manifest is unchanged and excluded columns are NULL.
     --writers  : number of file writers to use for Teradata exports
     --fastexporters
		: number of fast exporter sessions to use for Teradata exports
     --removedelimiters
		: performs character removal from CHAR/VARCHAR columns of 01 0A field and row delimiters.Slows down the data export.
     --encoding : specify encoding used (UTF8 | UTF16) supported, mainly used for wide unicode tables as UTF16 requires less bytes and is less subjective to 64KB rows overflow , optional otherwise
     --help     : this (help) message
     --debug    : persist intermediate files for debugging, prints more verbose status messages

    Examples:
        Export
        ======
        FULL : ./hdfs_exim.pl --export --source tdwc/datamover01_dba,... --table sandbox,campaigns_narrow --prefix /user/grp_gdoop_edw_rep_prod
        INCR.: ./hdfs_exim.pl --export --source tdwc/datamover01_dba,... --table prod_groupondw,fact_email_sends,send_date_key,20130723 --prefix /user/grp_gdoop_edw_rep_prod

        Import
        =====
        FULL : ./hdfs_exim.pl --import --target tdwc/datamover01_dba,.... --table  sandbox,campaigns_narrow --prefix /user/grp_gdoop_edw_rep_prod --importschema prod_groupondw
        -TABLE RECREATE (IMPORTANT: Target table will be dropped and recreated, so if you want to preserve existing data 
			and restore one partition use the APPEND option below and specify --stagingschema parameter
        INCR.: ./hdfs_exim.pl --import --target tdwc/datamover01_dba,.... --table  prod_groupondw,fact_tbl,send_key,20130723  --prefix /user/grp_gdoop_edw_rep_prod --importschema prod_groupondw
        -Table APPEND
        INCR.: ./hdfs_exim.pl --import --target tdwc/datamover01_dba,.... --table  prod_groupondw,fact_tbl,send_key,20130723  --prefix /user/grp_gdoop_edw_rep_prod --importschema prod_groupondw --stagingschema staging

        Notes:1.  If password contains reserved character ex. $, escape it twice : \\\$
              2.  BYTE/CLOB/GRAPHIC are not supported as delimited format for export / import, these columns are bypassed. Need to remove them from manifest file during import
EOF
        exit;
}

#############################


