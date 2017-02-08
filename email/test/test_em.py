#!/usr/local/bin/ python

"""
Usage:
  email.py [--source=<source>] [--target=<target>] [--eventDate=<eventDate>] [--eventDestination=<eventDestination>]
           [--offset=<offset>] [--countries=<countries>] [--assert] [--pre_assert] [--post_assert] [--pre_assert_only]
           [--post_assert_only] [--assert_only] [--debug] [-v|-vv]
  email.py (-h | --help)

Options:
  --source=<db>                          Specify which DB to use [default: grp_gdoop_pde.junohourly]
  --target=<table>                       Specify which Table name to use [default: svc_edw_dev_db.fact_email]
  --eventDate=<eventDate>                Specify what date to process by default it takes current date
  --eventDestination=<eventDestination>  Specify comma separated event destinations to process, by default it takes all
  --offset=<offset>                      Specify how many days to backfill from eventDate [default: 1]
  --countries=<countries>                Specify comma separated countries to process
  --assert                               Assertions Flag, whether to run Pre Assertions or not
  --pre_assert                           Run Core Process along with Pre Assertions
  --post_assert                          Run Core Process along with Post Assertions
  --pre_assert_only                      Run only Pre Assertions
  --post_assert_only                     Run only Post Assertions
  --assert_only                          Run only Assertions both Pre and Post
  --debug                                Debug flag
  -v|-vv                                 Logging level
"""

from docopt import docopt
from config import *
from os_util import *
from functools import reduce

import logging
import sys

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql.functions import upper, from_unixtime, lit, col, when, broadcast, datediff


def blank_as_null(x):
    """ Make Empty Cols to NULL """
    return when(col(x) != "", col(x)).otherwise(None)


def get_source_info():
    """
    Fetches all the needed info like columns, partitions, location, and format for Source

    This can be achieved either by running DESC FORMATTED on Hive or on Spark Sql

    Running Desc Formatted on Hive is slower than running on Spark Sql. Since Spark's output is messy
        and cannot be parsed we are using Hive

    :return: schema_info (all the relevant info of source)
    """
    logging.info('Fetching Schema Info for Source Table: %s' % get('source'))
    # schema_info = table_schema(get('source'))
    # set_arg('source_cols', schema_info['cols'])
    # set_arg('source_part_cols', schema_info['part_cols'])
    # set_arg('source_location', schema_info['location'])
    # set_arg('source_format', schema_info['format'])
    #
    # return schema_info

    # Alternatively to get location info fast you can execute below code
    source_location = re.findall("'(.*)'", str(spark.sql("DESC FORMATTED " + get('source'))
                                               .filter("col_name='Location:'").select("data_type").collect()[0]))[0]
    set_arg('source_location', source_location)


def create_df(event_dates, platform, event_destination):
    """
    For each event date, platform, and event destination it creates a data frame by reading
        respective source's HDFS files in parquet format

    If debug mode is ON then it stores the temp data on HDFS and creates Stage tables to debug

    :param event_dates: event dates to read
    :param platform: platform to read
    :param event_destination: event destination to read

    :return: df (data frame for that particular partition passed)
    """

    # We cannot dynamically generate the folder structure as partition cols names and
    # actual folder names are differing in cases
    # part = "/".join([i + "=" + str(eval(i)) for i in get('source_part_cols')])
    paths = []

    if event_destination == get_email('send'):
        d = (datetime.datetime.strptime(min(event_dates), '%Y-%M-%d') - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        event_dates.append(d)

    for event_date in event_dates:
        paths.append(get('source_location') +
                     get('source_partition_location').format(event_date=event_date, platform=platform,
                                                             event_dest=event_destination))

    logging.info('Creating Data Frame for %s, %s, %s' % (event_dates, platform, event_destination))

    df = spark.read.format(get('input_format')).load(paths)

    df = df.select(*(upper(df[col]).alias(col) if col == get('source_country_column') else col for col in
                     get(event_destination + '_cols'))) \
        .distinct() \
        .withColumn(get('stg_part_col')[0], from_unixtime(df[get('event_time_col')].substr(1, 10), format='yyyy-MM-dd'))

    if get('countries'):
        logging.info('Only Processing Countries %s' % ",".join(get('countries')))
        return df.filter(get('source_country_column') + " in " +
                         "(" + ','.join(['"' + i + '"' for i in get('countries')]) + ")" +
                         " and " + get_filter_condition(event_destination))
    else:
        return df


def get_event_dates_to_process():
    """
    If a user is passing an offset that means to back fill for certain number of days this will collect all the dates
        to be processed

    :return: event_dates (event dates to process)
    """
    logging.info('Analyzing event dates to be processed')
    event_dates = []
    for i in range(int(get('offset'))):
        start_date = datetime.datetime.strptime(get('eventDate'), '%Y-%m-%d')
        event_dates.append((start_date - datetime.timedelta(i)).strftime('%Y-%m-%d'))

    set_arg("event_dates", event_dates)

    logging.info("Event Dates to be processed are: %s " % get('event_dates'))
    return event_dates


def union_all(dfs):
    """
    Combine multiple data frames to single data frame


    :param dfs: List of DataFrames

    :return: Union of all DataFrames
    """
    logging.info('Performing Union on a list of Data Frames')
    return reduce(DataFrame.unionAll, dfs)


def pre_assertion_checks():
    """
    Running Pre Assertion checks
    :return: None
    """
    logging.info("Running the Pre Assertion Checks")
    start = datetime.datetime.now()

    for eventDestination in get('eventDestinations'):

        eh = "emailreceiveraddress" if eventDestination == get_email('bounce') else "emailhash"

        res = spark.sql("""SELECT {date_col}, COUNT(DISTINCT {cnt_col}) AS cnt FROM {table} GROUP BY {date_col}"""
                        .format(date_col="event_date", cnt_col=eh,
                                table=eventDestination + "_table")).collect()
        cnt_dict = {}
        for i in res:
            cnt_dict[i.event_date] = i.cnt

        set_arg(eventDestination + "_cnt", cnt_dict)

        logging.info('Count Dict %s' % get(eventDestination + "_cnt"))

    for event_date in get('event_dates'):
        for eventDestination in get('eventDestinations'):
            logging.info(
                '%s Count for %s: %s' % (eventDestination, event_date, get(eventDestination + "_cnt")[event_date]))
        if not get(get_email('delivery') + '_cnt')[event_date] > get(get_email('open') + '_cnt')[event_date]:
            raise Exception("Open count is greater than Delivery Count")
        if not get(get_email('delivery') + '_cnt')[event_date] > get(get_email('click') + '_cnt')[event_date]:
            raise Exception("Click count is greater than Delivery Count")
        if not get(get_email('open') + '_cnt')[event_date] > get(get_email('click') + '_cnt')[event_date]:
            raise Exception("Click count is greater than Open Count")

    end = datetime.datetime.now()
    time_took = end - start
    (minutes, seconds) = divmod(time_took.days * 86400 + time_took.seconds, 60)
    logging.info('The total time it took for Pre Assertions is %s Minutes and %s Seconds' % (minutes, seconds))


def post_assertion_checks():
    """
    Running Post Assertion checks
    :param final_table: Final Table on which the post assertions to run
    :return: None
    """
    logging.info("Running the Post Assertion Checks")
    start = datetime.datetime.now()

    paths = []
    for event_date in get('event_dates'):
        paths.append(get('final_location') + get('final_part_col')[0] + "=" + event_date)

    post_assert_df = spark.read.format(get('final_output_format')).option('basePath', get('final_location')).load(paths)
    post_assert_df.createOrReplaceTempView('post_assert_table')

    qry = """
            SELECT
                 {date_col},
                 SUM(send_cnt) AS send_cnt,
                 SUM(open_cnt) AS open_cnt,
                 SUM(click_cnt) AS click_cnt,
                 SUM(bounce_cnt) AS bounce_cnt,
                 COUNT(*) as cnt
            FROM {table}
            GROUP BY {date_col}
          """.format(date_col="event_date", table="post_assert_table")

    logging.info('Running Query %s' % qry)
    res = spark.sql(qry).collect()
    logging.info('Res %s' % res)
    for i in res:
        logging.info('Send Count for %s: %s' % (i.event_date, i.send_cnt))
        logging.info('Open Count for %s: %s' % (i.event_date, i.open_cnt))
        logging.info('Click Count for %s: %s' % (i.event_date, i.click_cnt))
        logging.info('Bounce Count for %s: %s' % (i.event_date, i.bounce_cnt))
        logging.info('Overall Count for %s: %s' % (i.event_date, i.cnt))

        if i.cnt == 0 or i.send_cnt == 0 or i.open_cnt == 0 or i.click_cnt == 0 or i.bounce_cnt == 0:
            pass
            # raise Exception("Count is ZERO, Please check the resulting data")

    end = datetime.datetime.now()
    time_took = end - start
    (minutes, seconds) = divmod(time_took.days * 86400 + time_took.seconds, 60)
    logging.info('The total time it took for Post Assertions is %s Minutes and %s Seconds' % (minutes, seconds))


def remove_existing_hdfs_folders(loc, job):
    """
    Removing existing locations in HDFS for the Data to be overwritten
    :param loc: Location of HDFS to be removed
    :return: None
    """
    if job == 'stage':
        part_col = get('stg_part_col')
    else:
        part_col = get('final_part_col')

    for eventDate in get('event_dates'):
        if len(get('countries')) != get('num_countries'):
            for country in get('countries'):
                hdfs_rm(loc + part_col[0] + "=" + eventDate + "/" + part_col[1] + "=" + country)
        else:
            hdfs_rm(loc + part_col[0] + "=" + eventDate)


def remove_and_move_hdfs_folders(final_loc, tmp_loc):
    """
    Removing existing locations in HDFS and move the data from temp location to final
    :param final_loc: Location of HDFS to be removed and moved to
    :param tmp_loc: Temporary location of the data
    :return: None
    """
    part_col = get('final_part_col')

    for eventDate in get('event_dates'):
        if len(get('countries')) != get('num_countries'):
            hdfs_mkdir(final_loc + part_col[0] + "=" + eventDate)
            for country in get('countries'):
                hdfs_rm(final_loc + part_col[0] + "=" + eventDate + "/" + part_col[1] + "=" + country)

            hdfs_mv(tmp_loc + part_col[0] + "=" + eventDate + "/*", final_loc + part_col[0] + "=" + eventDate + "/")
        else:
            hdfs_rm(final_loc + part_col[0] + "=" + eventDate)
            hdfs_mv(tmp_loc + part_col[0] + "=" + eventDate, final_loc)


def create_data_frames_to_be_read():
    """
    Creating Dataframes to be Read for specified event dates and event destinations.
    :return: None
    """

    get_source_info()

    platform = get('platform')
    event_dates = get('event_dates')

    logging.info('Creating Data Frames for partitions to be read')
    for eventDestination in get('eventDestinations'):

        set_arg(eventDestination + "_df", create_df(event_dates[:], platform, eventDestination))

        # Currently, we are writing the temporary data into stage tables we can make it optional for debug purpose
        if get('--debug'):
            stg_table = "{db}.stg_{table}".format(db=get('target_db'), table=eventDestination)
            logging.info('Creating Staging table for {stg_table}'.format(stg_table=stg_table))
            # stg_loc = get('stage_location') + eventDestination + "_stg"
            stg_loc = get('stage_location') + "stg_" + eventDestination
            remove_existing_hdfs_folders(stg_loc, 'stage')
            get(eventDestination + "_df").write.save(stg_loc, format=get('stg_output_format'), mode='append',
                                                     partitionBy=(get('stg_part_col')[0], get('stg_part_col')[1]))
            # mode("append").partitionBy(get('stg_part_col')[0], get('stg_part_col')[1]).parquet(stg_loc)

            col_schema = ",".join(
                [tup[0] + " " + tup[1] for tup in get(eventDestination + "_df").dtypes if
                 tup[0] not in get('stg_part_col')])

            # Extract Staging tables Info if already exists
            stg_schema_info = table_schema("{stg_table}".format(stg_table=stg_table))
            stg_cols = sorted(
                i.lower() for i in (stg_schema_info['cols'].keys() + [get('stg_part_col')[1]]))
            supplied_cols = sorted([i.lower() for i in get(eventDestination + '_cols')])

            if not stg_schema_info['cols'] or stg_cols != supplied_cols:
                logging.info('Staging table does not exist or DDL Mismatch happened, Creating a new Staging Table')
                spark.sql("DROP TABLE IF EXISTS {stg_table}".format(stg_table=stg_table))
                spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS {stg_table} (  {col_schema}  ) "
                          "PARTITIONED BY ({stg_part_col_0} string, {stg_part_col_1} string) STORED AS {output_format} "
                          "LOCATION '{stg_loc}'".format(stg_table=stg_table, col_schema=col_schema,
                                                        stg_part_col_0=get('stg_part_col')[0],
                                                        stg_part_col_1=get('stg_part_col')[1],
                                                        output_format=get('stg_output_format'), stg_loc=stg_loc))

            logging.info('Staging table Exists, Altering the partitions')

            for eventDate in event_dates:
                for country in get('countries'):
                    spark.sql("ALTER TABLE {stg_table} ADD IF NOT EXISTS "
                              "PARTITION ({stg_part_col_0}='{eventDate}', {stg_part_col_1}='{country}')"
                              .format(stg_table=stg_table, stg_part_col_0=get('stg_part_col')[0],
                                      stg_part_col_1=get('stg_part_col')[1],
                                      eventDate=eventDate, country=country))

        logging.info('Creating Temp View in Spark Sql for %s' % (eventDestination + "_table"))
        # get(eventDestination + "_df").cache()
        get(eventDestination + "_df").createOrReplaceTempView(eventDestination + "_table")


def run_core_process():
    """
    This function does all the heavy lifting work
        1. For each event destination and for each event date it unions all the DataFrames
        2. Once the DataFrames are created it creates temporary views in Spark Sql
        3. Runs the ETL Logic and stores it in a DataFrame
        4. Saves the resulting DF to final Fact Table's HDFS path in ORC/Snappy
        5. Creates final Fact table if not exists
        6. Alters or adds the partitions it processed to Final Fact table
        7. Creates Done files and updates metadata

    :return: None
    """
    logging.info('Running Core Process')
    create_data_frames_to_be_read()

    if get('--assert') or get('--pre_assert'):
        pre_assertion_checks()

    qry = """
            SELECT
                  emailreceiveraddress
                 ,emailsendid
                 ,country
                 ,MIN(emailsubject) AS emailsubject
                 ,MIN(campaigngroup) AS campaigngroup
                 ,MIN(businessgroup) AS businessgroup
            FROM {send}_table s
            GROUP BY 1,2,3
          """.format(send=get_email('send'))

    send_data = spark.sql(qry)
    send_data.createOrReplaceTempView("send_table")

    qry = """
            SELECT
                 d.emailsendid
                ,SHA2(CONCAT('{email_salt}', d.emailreceiveraddress), 256) AS emailhash
                ,d.country
                ,MIN(from_unixtime(CAST(substr(d.eventTime,1,10) AS INT),'yyyy-MM-dd HH:mm:ss')) AS event_time
                ,MIN(d.event_date) AS event_date
                ,MIN(emailname) AS emailname
                ,MIN(emailsubject) AS emailsubject
                ,MIN(campaigngroup) AS campaigngroup
                ,MIN(businessgroup) AS businessgroup
                ,COUNT(*) AS send_cnt
            FROM {delivery}_table d
            LEFT OUTER JOIN send_table s
            ON d.emailreceiveraddress = s.emailreceiveraddress AND d.emailsendid = s.emailsendid AND d.country = s.country
            GROUP BY 1,2,3
        """.format(email_salt=get('email_salt'), delivery=get_email('delivery'))

    send_delivery = spark.sql(qry)
    send_delivery.createOrReplaceTempView("send_delivery_table")

    qry = """
            SELECT
                 emailsendid
                 ,emailhash
                 ,country
                 ,MIN(event_date) AS event_date
                 ,MIN(useragent) AS useragent
                 ,COUNT(eventtime) AS open_cnt
            FROM {open}_table
            GROUP BY 1,2,3
          """.format(open=get_email('open'))

    opens = spark.sql(qry)
    opens.createOrReplaceTempView("open_table")

    qry = """
            SELECT
                 emailsendid
                 ,emailhash
                 ,country
                 ,MIN(event_date) AS event_date
                 ,MIN(useragent) AS useragent
                 ,COUNT(eventtime) AS click_cnt
                 ,SUM(CASE WHEN clickDestination LIKE '%unsub%' THEN 1 ELSE 0 END) AS unsub_cnt
            FROM {click}_table
            GROUP BY 1,2,3
          """.format(click=get_email('click'))

    clicks = spark.sql(qry)
    clicks.createOrReplaceTempView("click_table")

    qry = """
            SELECT
                 emailsendid
                 ,SHA2(CONCAT('{email_salt}', emailreceiveraddress), 256) AS emailhash
                 ,country
                 ,CASE WHEN bouncecategory IN ('1 Undetermined','40 Generic Bounce','50 Mail Block','100 Challenge-Response','52 Spam Content','54 Relaying Denied','51 Spam Block') THEN 1
 	                   WHEN bouncecategory IN ('70 Transient Failure','20 Soft Bounce','24 Timeout','23 Too Large','25 Admin Failure','60 Auto-Reply','21 DNS Failure','22 Mailbox Full') THEN 2
 	                   WHEN bouncecategory IN ('10 Invalid Recipient','90 Unsubscribe','30 Generic Bounce No RCPT') THEN 3
 	                   WHEN sourcetopicname LIKE 'msys_fbl%' OR sourcetopicname LIKE 'msys_listunsub%' THEN 4
	                   ELSE NULL END AS  email_bounce_type_id
                 ,MIN(event_date) as event_date
            FROM {bounce}_table
            GROUP BY 1,2,3,4
          """.format(email_salt=get('email_salt'), bounce=get_email('bounce'))

    bounces = spark.sql(qry)
    bounces.createOrReplaceTempView("bounce_data")

    qry = """
            SELECT
                 emailsendid
                 ,emailhash
                 ,country
                 ,MIN(CASE WHEN b.email_bounce_type_id=4 THEN b.event_date ELSE NULL END) AS first_complaint_date
                 ,MIN(CASE WHEN b.email_bounce_type_id IN (1,2) THEN b.event_date ELSE NULL END) AS first_softbounce_date
                 ,MIN(CASE WHEN b.email_bounce_type_id=3 THEN b.event_date ELSE NULL END) AS first_hardbounce_date
                 ,SUM(CASE WHEN b.email_bounce_type_id=4 THEN 1 ELSE 0 END) AS complaint_cnt
                 ,SUM(CASE WHEN b.email_bounce_type_id IN (1,2,3) THEN 1 ELSE 0 END) AS bounce_cnt
                 ,SUM(CASE WHEN b.email_bounce_type_id IN (1,2) THEN 1 ELSE 0 END) AS soft_bounce_cnt
                 ,SUM(CASE WHEN b.email_bounce_type_id=3 THEN 1 ELSE 0 END) AS hard_bounce_cnt
            FROM bounce_data b
            GROUP BY 1,2,3
          """

    bounce_table = spark.sql(qry)
    bounce_table.createOrReplaceTempView("bounce_table")

    final_qry = """
            SELECT
                u.uuid AS user_uuid
                ,s.emailsendid AS send_id
                ,s.country AS country
                ,s.event_date AS event_date
                ,s.country AS country_code
                ,s.event_date AS send_date
                ,s.event_time AS send_timestamp
                ,s.emailname AS email_name
                ,s.emailsubject AS email_subject
                ,s.campaigngroup AS campaign_group
                ,s.businessgroup AS business_group
                ,o.useragent AS first_open_user_agent
                ,c.useragent AS first_click_user_agent
                ,o.event_date AS first_open_date
                ,c.event_date AS first_click_date
                ,s.send_cnt AS send_cnt
                ,o.open_cnt AS open_cnt
                ,c.click_cnt AS click_cnt
                ,c.unsub_cnt AS unsub_cnt
                ,CASE WHEN s.event_date = o.event_date THEN 1 ELSE 0 END AS sd_open_cnt
                ,CASE WHEN s.event_date = c.event_date THEN 1 ELSE 0 END AS sd_click_cnt
                ,CASE WHEN datediff(o.event_date, s.event_date) BETWEEN 0 AND 2 THEN 1 ELSE 0 END AS d3_open_cnt
                ,CASE WHEN datediff(o.event_date, s.event_date) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS d7_open_cnt
                ,CASE WHEN datediff(c.event_date, s.event_date) BETWEEN 0 AND 2 THEN 1 ELSE 0 END AS d3_click_cnt
                ,CASE WHEN datediff(c.event_date, s.event_date) BETWEEN 0 AND 6 THEN 1 ELSE 0 END AS d7_click_cnt
                ,b.first_complaint_date AS first_complaint_date
                ,b.first_softbounce_date AS first_softbounce_date
                ,b.first_hardbounce_date AS first_hardbounce_date
                ,b.complaint_cnt AS complaint_cnt
                ,b.bounce_cnt AS bounce_cnt
                ,b.soft_bounce_cnt AS soft_bounce_cnt
                ,b.hard_bounce_cnt AS hard_bounce_cnt
            FROM send_delivery_table s
            LEFT OUTER JOIN prod_groupondw.gbl_dim_user_uniq u
            ON s.emailhash=u.encrypted_login_email AND s.country=u.country_code
            LEFT OUTER JOIN open_table o
            ON s.emailsendid = o.emailsendid  AND s.country = o.country AND s.emailhash = o.emailhash
            LEFT OUTER JOIN click_table c
            ON s.emailsendid = c.emailsendid  AND s.country = c.country AND s.emailhash = c.emailhash
            LEFT OUTER JOIN bounce_table b
            ON s.emailsendid = b.emailsendid  AND s.country = b.country AND s.emailhash = b.emailhash
          """

    final_cols = OrderedDict()
    p = re.compile("^.* AS (.*).*$")
    for l in final_qry.splitlines():
        m = p.match(l)
        if m:
            col = m.group(1)
            if col not in get('final_part_col'):
                if col.endswith("_cnt"):
                    final_cols[col] = "bigint"
                else:
                    final_cols[col] = "string"

    res = spark.sql(final_qry)
    final_table = get('final_table')

    final_loc = get('final_location')
    tmp_loc = get('tmp_location')

    # remove_existing_hdfs_folders(final_loc, 'final')
    hdfs_rm(tmp_loc)
    logging.info("Running the Actual JOIN to populate AGG Table")
    start = datetime.datetime.now()

    res.coalesce(get('output_num_files')).write.save(path=tmp_loc, format=get('final_output_format'), mode='append',
                                                     partitionBy=(get('final_part_col')[0], get('final_part_col')[1]))

    end = datetime.datetime.now()
    time_took = end - start
    (minutes, seconds) = divmod(time_took.days * 86400 + time_took.seconds, 60)
    logging.info(
        'The total time it took for OverAll Join process to finish is %s Minutes and %s Seconds' % (minutes, seconds))

    remove_and_move_hdfs_folders(final_loc, tmp_loc)

    final_schema_info = table_schema("{final_table}".format(final_table=final_table))

    existing_final_cols = sorted(
        i.lower() for i in (final_schema_info['cols'].keys() + final_schema_info['part_cols'].keys()))
    supplied_cols = sorted([i.lower() for i in final_cols.keys()] + get('final_part_col'))

    col_schema = ",".join([i[0] + " " + i[1] for i in zip(final_cols.keys(), final_cols.values())])

    if not final_schema_info['cols'] or existing_final_cols != supplied_cols:
        logging.info('Final table does not exist or DDL Mismatch happened, Creating a new Final Table')
        spark.sql("DROP TABLE IF EXISTS {final_table}".format(final_table=final_table))
        spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS {final_table} (  {col_schema}  ) "
                  "PARTITIONED BY ({final_part_col_0} string, {final_part_col_1} string) STORED AS {output_format} "
                  "LOCATION '{final_loc}'".format(final_table=final_table, col_schema=col_schema,
                                                  final_part_col_0=get('final_part_col')[0],
                                                  final_part_col_1=get('final_part_col')[1],
                                                  output_format=get('final_output_format'), final_loc=final_loc))
    logging.info('Final table Exists, Altering the partitions')

    for eventDate in get('event_dates'):
        for country in get('countries'):
            spark.sql("ALTER TABLE {final_table} ADD IF NOT EXISTS "
                      "PARTITION ({final_part_col_0}='{eventDate}', {final_part_col_1}='{country}')"
                      .format(final_table=final_table, final_part_col_0=get('final_part_col')[0],
                              final_part_col_1=get('final_part_col')[1],
                              eventDate=eventDate, country=country))

    if get('--assert') or get('--post_assert'):
        post_assertion_checks()


def kickoff():
    """
    Actual KickOff
    """
    # df = spark.read.parquet('hdfs://gdoop-namenode/user/grp_gdoop_platform-data-eng/prod/juno/junoHourly/eventDate=2017-01-11/platform=email/eventDestination=emailDelivery')
    # df.registerTempTable("df")
    # res=spark.sql("select distinct a.emailreceiveraddress, SHA2(CONCAT('ph5p6uTezuwr4c8aprux', a.emailreceiveraddress), 256) as emailhash, country from df a LEFT OUTER JOIN prod_groupondw.gbl_dim_user_uniq b ON SHA2(CONCAT('ph5p6uTezuwr4c8aprux', a.emailreceiveraddress), 256) = b.encrypted_login_email WHERE b.uuid is null")
    # res.write.orc('/user/grp_gdoop_edw_etl_dev/email/emailhash_missing', mode='overwrite')
    res=spark.sql("select a.emailhash as emailreceiveraddress, SHA2(CONCAT('ph5p6uTezuwr4c8aprux', a.emailhash), 256) as emailhash,a.emailsendid, a.eventdate  from svc_edw_dev_db.kafka_send_data_0121_0125 a LEFT OUTER JOIN svc_edw_dev_db.juno_send_data_0121_0125 b ON a.emailhash = b.emailhash and a.emailsendid = b.emailsendid and a.eventdate = b.eventdate WHERE b.emailhash is null and b.emailsendid is null and b.eventdate is null")
    res.write.orc('/user/grp_gdoop_edw_etl_dev/email/juno_kafka_diff_0121_0125', mode='overwrite')

if __name__ == '__main__':
    conf = (SparkConf()
            .setAppName("EmailPipeline")
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.dynamicAllocation.enabled", "False")
            .set("spark.sql.shuffle.partitions", 50))

    spark = SparkSession.builder \
        .master("yarn") \
        .appName("EmailPipeline") \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    arg = docopt(__doc__)
    config = load_conf(arg)
    level_map = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(level_map[config['-v']])
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)
    logging.debug('Args [%s]' % arg)

    start = datetime.datetime.now()
    kickoff()
    end = datetime.datetime.now()
    time_took = end - start
    (minutes, seconds) = divmod(time_took.days * 86400 + time_took.seconds, 60)
    logging.info('Total Runtime is %s Minutes and %s Seconds' % (minutes, seconds))

    spark.stop()
