import sys
import argparse
from datetime import date, datetime, timedelta
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, LongType
from pyspark.sql.functions import broadcast
import pytz
from pyspark.sql.functions import array_join
from pyspark.sql.functions import arrays_zip
from pyspark.sql.functions import col
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import count
from pyspark.sql.functions import expr,concat_ws
from pyspark.sql.functions import lit, when, array
from pyspark.sql.functions import lower
from pyspark.sql.functions import min
from pyspark.sql.types import StringType, MapType
'''
notes: do not add distinct on the final store stage in order to avoid de-depulication
email opens/clicks/spams/unsubscribes happened within a second are de-duped in the email extract pipeline, 
because the precision of data is being changed from yyyy-mm-dd hh:mi:ss.mmm from the source (SFMC) to yyyy-mm-dd hh:mi:ss at the cleansing layer.
'''
if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("fact_marketing_email_response_siena") \
        .config("spark.memory.useLegacyMode", "false") \
        .config("spark.default.parallelism", "2000") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.port.maxRetries", "32") \
        .config("spark.rdd.compress", "true") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.speculation", "true") \
        .config("spark.executor.cores","5")\
        .config("spark.driver.cores","5")\
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .config("spark.sql.broadcastTimeout",  "3600") \
        .config("spark.driver.memoryOverhead","5120")\
        .config("spark.executor.memoryOverhead","5120") \
        .config("spark.network.timeout","600") \
        .config("spark.rpc.io.serverTreads","64") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.speculation","true") \
        .config("spark.speculation.interval","1s") \
        .config("spark.speculation.quantile","0.75") \
        .config("spark.speculation.multiplier","1.5") \
        .enableHiveSupport() \
        .getOrCreate()


    parser = argparse.ArgumentParser()
    parser.add_argument("--email_open_date", help="customer sent email open date")
    parser.add_argument("--email_open_end_date", help="customer sent email open end date")
    parser.add_argument("--email_send_start_date", help="send start date, it's 2013-01-01")
    parser.add_argument("--email_end_date", help="30 days from open date")
    parser.add_argument("--year", help="siena snapshot year with format YYYY")
    parser.add_argument("--month", help="siena snapshot month ith format MM")
    parser.add_argument("--day", help="siena snapshot day ith format DD")
    args = parser.parse_args()

    EMAIL_OPEN_DATE = args.email_open_date
    EMAIL_OPEN_END_DATE = args.email_open_end_date
    EMAIL_SEND_START_DATE = args.email_send_start_date
    EMAIL_END_DATE = args.email_end_date

# sfmc is the increamental process, uses to filter the needed data 
    SFMC_YEAR=EMAIL_OPEN_DATE[0:4] # get year of the open date format yyyy
    SEND_YEAR= EMAIL_SEND_START_DATE[0:4]
    
    if(EMAIL_OPEN_DATE[5:7]==12 and EMAIL_END_DATE[5:7]==1):
        SFMC_MONTH=EMAIL_END_DATE[5:7]
    else:
        SFMC_MONTH=EMAIL_OPEN_DATE[5:7] # get month of the open date format mm
        
    YEAR=args.year
    MONTH=args.month
    DAY=args.day

# get a list of internal customer_ids from csv file
    df_blacklist= spark.read.csv('s3://gd-ckpetlbatch-prod-code/enterprise_data/email_marketing/sfmc_blacklist.csv', header = True)

    
    df_events_sent=spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  email_template_name as email_name,
                                  year,
                                  month,
                                  day,
                                  email_template_id as email_id from sfmc_email_events_cln.sfmc_email_events_sent_cln """)

    df_events_sent=df_events_sent.filter(df_events_sent.year>=SEND_YEAR)

  

    df_events_sent_log=spark.sql(""" select sfmc_job_id, 
                                     sfmc_list_id, 
                                     sfmc_batch_id, 
                                     sfmc_subscriber_id,
                                     locale_code,
                                     event_utc_ts,
                                     customer_id,
                                     internal_source_code,
                                     cms_omni_campaign_id as campaign_id,
                                     cms_omni_segment_id as segment_id,
                                     cms_omni_channel_id as channel_id,
                                     private_label_id,
                                     year,
                                     month,
                                     day
                                    from sfmc_email_events_cln.sfmc_email_events_send_log_cln """)

    df_events_sent_log=df_events_sent_log.filter(df_events_sent_log.year>=SEND_YEAR)


    df_events_open= spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  LOWER(event_type_code) AS email_response_type,
                                  event_unique_flag as first_open_flag ,
                                  year,
                                  month,
                                  day
                                  from sfmc_email_events_cln.sfmc_email_events_open_cln """)
    #filter by needed open date

    df_events_open=df_events_open.filter(df_events_open.year>=SFMC_YEAR)\
    .filter(df_events_open.month>=SFMC_MONTH)

    
    df_events_bounce= spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  LOWER(bounce_category) as email_response_type,
                                  year,
                                  month,
                                  day
                                  from sfmc_email_events_cln.sfmc_email_events_bounce_cln """)
    #filter by needed bounce date

    df_events_bounce=df_events_bounce.filter(df_events_bounce.year>=SFMC_YEAR)\
    .filter(df_events_bounce.month>=SFMC_MONTH)
    
    df_events_click= spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  LOWER(event_type_code) as email_response_type,
                                  event_unique_flag as first_click_flag,
                                  url as response_url_text,
                                  link_name as response_link_name,
                                  link_content as response_link_context_text,
                                  year,
                                  month,
                                  day
                                  from sfmc_email_events_cln.sfmc_email_events_click_cln """)
    #filter by needed click date

    df_events_click=df_events_click.filter(df_events_click.year>=SFMC_YEAR)\
    .filter(df_events_click.month>=SFMC_MONTH)

    

    df_events_spam= spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  LOWER(event_type_code) as email_response_type,
                                  year,
                                  month,
                                  day
                                  from sfmc_email_events_cln.sfmc_email_events_spam_cln """)

    #filter by needed spam date
    df_events_spam=df_events_spam.filter(df_events_spam.year>=SFMC_YEAR)\
    .filter(df_events_spam.month>=SFMC_MONTH)
    
    df_events_unsubscribe= spark.sql(""" select sfmc_job_id, 
                                  sfmc_list_id, 
                                  sfmc_batch_id, 
                                  sfmc_subscriber_id,
                                  event_utc_ts,
                                  LOWER(event_type_code) as email_response_type,
                                  year,
                                  month,
                                  day
                                  from sfmc_email_events_cln.sfmc_email_events_unsubscribe_cln """)

    df_events_unsubscribe=df_events_unsubscribe.filter(df_events_unsubscribe.year>=SFMC_YEAR)\
    .filter(df_events_unsubscribe.month>=SFMC_MONTH)
    
# load needed date from hadoop-legacy-data buckect by WANdisco

    df_fortknox_shopper = spark.sql('select shopper_id, UPPER(country) AS countrycode from fortknox.fortknox_shopper_snap')
    

    # Get the max build time of customer_ids table. We need any build not necessarily the max
    max_bld = spark.sql('show partitions customers.customer_ids').collect()[-1]['partition'].split('=')[1]

    customer_view= '''
    select id, customerid from customers.customer_ids where build_ts = '{max_build_ts}'
    '''.format(max_build_ts=max_bld)

    df_customer= spark.sql(customer_view).distinct()

    #filer siena data 

    fill_events_sent_log= df_events_sent_log.withColumn('send_mst_ts', (F.from_utc_timestamp(df_events_sent_log.event_utc_ts, "MST"))).drop(df_events_sent_log.event_utc_ts)

    filter_events_sent_log= fill_events_sent_log.filter(fill_events_sent_log.send_mst_ts >= EMAIL_SEND_START_DATE).filter(fill_events_sent_log.send_mst_ts <= EMAIL_END_DATE)

    fill_events_sent= df_events_sent.withColumn('send_mst_ts', (F.from_utc_timestamp(df_events_sent.event_utc_ts, "MST"))).drop(df_events_sent.event_utc_ts)


    filter_events_sent= fill_events_sent.filter(fill_events_sent.send_mst_ts >= EMAIL_SEND_START_DATE).filter(fill_events_sent.send_mst_ts <= EMAIL_END_DATE)

#siena response data

    fill_events_open= df_events_open.withColumn('response_mst_ts', (F.from_utc_timestamp(df_events_open.event_utc_ts, "MST"))).drop(df_events_open.event_utc_ts)


    filter_events_open= fill_events_open.filter(fill_events_open.response_mst_ts >= EMAIL_OPEN_DATE).filter(fill_events_open.response_mst_ts <= EMAIL_OPEN_END_DATE)\
    .withColumn('email_response_code',F.lit('o').cast(StringType()))\
    .withColumn('first_click_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_complaint_flag',F.lit(None).cast('boolean'))\
    .withColumn('response_url_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_context_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_name',F.lit(None).cast(StringType()))\
    .select('sfmc_job_id',
            'sfmc_list_id',
            'sfmc_batch_id',
            'sfmc_subscriber_id',
            'email_response_type',
            'email_response_code',
            'first_open_flag',
            'first_click_flag',
            'first_complaint_flag',
            'response_url_text',
            'response_link_name',
            'response_link_context_text',
            'response_mst_ts') #76271


    fill_events_bounce= df_events_bounce.withColumn('response_mst_ts', (F.from_utc_timestamp(df_events_bounce.event_utc_ts, "MST"))).drop(df_events_bounce.event_utc_ts)

    #email_response_code is the abbrevation of the first charater of the each string in string array split by space, e.g soft bounce  as sb
    filter_events_bounce= fill_events_bounce.filter(fill_events_bounce.response_mst_ts >= EMAIL_OPEN_DATE).filter(fill_events_bounce.response_mst_ts <= EMAIL_END_DATE)\
     .withColumn('email_response_code',F.concat(F.substring(F.split(fill_events_bounce.email_response_type,' ').getItem(0),1,1),F.substring(F.split(fill_events_bounce.email_response_type,' ').getItem(1),1,1)))\
     .withColumn('first_click_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_complaint_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_open_flag',F.lit(None).cast('boolean'))\
    .withColumn('response_url_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_context_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_name',F.lit(None).cast(StringType()))\
    .select('sfmc_job_id',
            'sfmc_list_id',
            'sfmc_batch_id',
            'sfmc_subscriber_id',
            'email_response_type',
            'email_response_code',
            'first_open_flag',
            'first_click_flag',
            'first_complaint_flag',
            'response_url_text',
            'response_link_name',
            'response_link_context_text',
            'response_mst_ts') #283


    fill_events_click= df_events_click.withColumn('response_mst_ts', (F.from_utc_timestamp(df_events_click.event_utc_ts, "MST"))).drop(df_events_click.event_utc_ts)
    

    filter_events_click= fill_events_click.filter(fill_events_click.response_mst_ts >= EMAIL_OPEN_DATE).filter(fill_events_click.response_mst_ts <= EMAIL_OPEN_END_DATE)\
    .withColumn('email_response_code',F.lit('c').cast(StringType()))\
    .withColumn('first_complaint_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_open_flag',F.lit(None).cast('boolean'))\
    .select('sfmc_job_id',
            'sfmc_list_id',
            'sfmc_batch_id',
            'sfmc_subscriber_id',
            'email_response_type',
            'email_response_code',
            'first_open_flag',
            'first_click_flag',
            'first_complaint_flag',
            'response_url_text',
            'response_link_name',
            'response_link_context_text',
            'response_mst_ts') #2462


    fill_events_spam= df_events_spam.withColumn('response_mst_ts', (F.from_utc_timestamp(df_events_spam.event_utc_ts, "MST"))).drop(df_events_spam.event_utc_ts)
    

    filter_events_spam= fill_events_spam.filter(fill_events_spam.response_mst_ts >= EMAIL_OPEN_DATE).filter(fill_events_spam.response_mst_ts <= EMAIL_OPEN_END_DATE)\
    .withColumn('email_response_code',F.lit('s').cast(StringType()))\
    .withColumn('first_click_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_complaint_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_open_flag',F.lit(None).cast('boolean'))\
    .withColumn('response_url_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_context_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_name',F.lit(None).cast(StringType()))\
    .select('sfmc_job_id',
            'sfmc_list_id',
            'sfmc_batch_id',
            'sfmc_subscriber_id',
            'email_response_type',
            'email_response_code',
            'first_open_flag',
            'first_click_flag',
            'first_complaint_flag',
            'response_url_text',
            'response_link_name',
            'response_link_context_text',
            'response_mst_ts')#26

    fill_events_unsubscribe= df_events_unsubscribe.withColumn('response_mst_ts', (F.from_utc_timestamp(df_events_unsubscribe.event_utc_ts, "MST"))).drop(df_events_unsubscribe.event_utc_ts)
    

    filter_events_unsubscribe= fill_events_unsubscribe.filter(fill_events_unsubscribe.response_mst_ts >= EMAIL_OPEN_DATE).filter(fill_events_unsubscribe.response_mst_ts <= EMAIL_OPEN_END_DATE)\
    .withColumn('email_response_code',F.lit('u').cast(StringType()))\
    .withColumn('first_click_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_complaint_flag',F.lit(None).cast('boolean'))\
    .withColumn('first_open_flag',F.lit(None).cast('boolean'))\
    .withColumn('response_url_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_context_text',F.lit(None).cast(StringType()))\
    .withColumn('response_link_name',F.lit(None).cast(StringType()))\
    .select('sfmc_job_id',
            'sfmc_list_id',
            'sfmc_batch_id',
            'sfmc_subscriber_id',
            'email_response_type',
            'email_response_code',
            'first_open_flag',
            'first_click_flag',
            'first_complaint_flag',
            'response_url_text',
            'response_link_name',
            'response_link_context_text',
            'response_mst_ts') #158
    

    union_response= filter_events_open.union(filter_events_bounce)\
    .union(filter_events_click).union(filter_events_spam)\
    .union(filter_events_unsubscribe)#79200


    cond= [filter_events_sent.sfmc_job_id==filter_events_sent_log.sfmc_job_id,filter_events_sent.sfmc_list_id==filter_events_sent_log.sfmc_list_id,
          filter_events_sent.sfmc_batch_id==filter_events_sent_log.sfmc_batch_id,
          filter_events_sent.sfmc_subscriber_id==filter_events_sent_log.sfmc_subscriber_id]

    email_send= filter_events_sent.join(filter_events_sent_log,cond,'left').drop(filter_events_sent_log.send_mst_ts)\
    .drop(filter_events_sent_log.sfmc_subscriber_id).drop(filter_events_sent_log.sfmc_list_id).drop(filter_events_sent_log.sfmc_batch_id)\
    .drop(filter_events_sent_log.sfmc_job_id)

    

    cond1= [email_send.sfmc_job_id==union_response.sfmc_job_id,email_send.sfmc_list_id==union_response.sfmc_list_id,
          email_send.sfmc_batch_id==union_response.sfmc_batch_id,
          email_send.sfmc_subscriber_id==union_response.sfmc_subscriber_id]

    join_send_response= union_response \
                                .join(email_send, cond1, 'left')\
                                .drop(email_send.sfmc_job_id)\
                                .drop(email_send.sfmc_list_id)\
                                .drop(email_send.sfmc_subscriber_id)\
                                .drop(email_send.sfmc_batch_id)

    # take the advantage of the exclude_reason_desc col, if the customer_id is interal, exclude_reason_desc is internal, otherwise it will be null
    exclude_col= F.when(df_blacklist.QaCustomerID.isNull(), F.lit(None).cast(StringType())).otherwise('internal customer')
    
    exclude_black_list = join_send_response.join(df_blacklist, join_send_response.customer_id==df_blacklist.QaCustomerID, 'left')\
    .withColumn('exclude_reason_desc',exclude_col)

    join_customer= exclude_black_list.join(df_customer,df_customer.customerid==exclude_black_list.customer_id,'left').drop(df_customer.customerid)
    #79200

    clean_response= join_customer.join(df_fortknox_shopper, df_fortknox_shopper.shopper_id==join_customer.id, 'left')\
                                    .select('send_mst_ts',
                                        'response_mst_ts',
                                     join_customer.email_id.alias('template_id'),
                                     join_customer.internal_source_code.alias('isc_source_code'),
                                     join_customer.id.alias('shopper_id'),
                                     df_fortknox_shopper.countrycode.alias('shopper_country_code'),
                                     'sfmc_job_id',
                                     'send_mst_ts',
                                     'sfmc_list_id',
                                     'sfmc_batch_id',
                                     'sfmc_subscriber_id',
                                     'first_complaint_flag',
                                     'first_open_flag',
                                     'first_click_flag',
                                     'email_response_type',
                                     'email_response_code',
                                     'response_url_text',
                                     'response_link_name',
                                     'response_link_context_text',
                                     'campaign_id',
                                     'segment_id',
                                     'channel_id',
                                     'email_name',
                                     'exclude_reason_desc',
                                     'private_label_id',
                                     F.split(join_customer.locale_code,'-').getItem(0).alias('email_language_code'),
                                     F.split(join_customer.locale_code,'-').getItem(1).alias('email_country_code'))\
                                     .withColumn('email_send_id', F.sha2(F.concat_ws('|', 'sfmc_job_id','sfmc_list_id','sfmc_batch_id','sfmc_subscriber_id'), 256))\
                                     .withColumn('email_id',F.lit(None).cast(StringType()))\
    .withColumn('email_message_guid',F.lit(None).cast(StringType()))\
    .withColumn('shopper_email_domain_name',F.lit(None).cast(StringType()))\
    .withColumn('template_name',F.lit(None).cast(StringType()))\
    .withColumn('email_control_flag',F.lit(None).cast('boolean'))\
    .withColumn('content_block_id',F.lit(None).cast(IntegerType()))\
    .withColumn('content_block_key_id',F.lit(None).cast(IntegerType()))\
    .withColumn('content_block_key_name', F.lit(None).cast(StringType()))\
    .withColumn('content_block_category_id',F.lit(None).cast(IntegerType()))\
    .withColumn('content_block_category_name', F.lit(None).cast(StringType()))\
    .withColumn('content_block_subcategory_id',F.lit(None).cast(IntegerType()))\
    .withColumn('content_block_subcategory_name',F.lit(None).cast(StringType()))\
    .withColumn('email_send_mst_date',F.date_format(join_customer.send_mst_ts, 'yyyy-MM-dd'))\
    .withColumn('email_response_mst_date',F.date_format(join_customer.response_mst_ts, 'yyyy-MM-dd'))\
    .withColumn('email_source_system_name',F.lit('sfmc').cast(StringType()))\
    .withColumn('content_block_click_eid_text',F.lit(None).cast(StringType()))\
    .withColumn('content_block_click_visit_guid',F.lit(None).cast(StringType()))\
    .withColumn('content_block_click_visitor_guid',F.lit(None).cast(StringType()))\
    .withColumn('etl_build_mst_ts', (F.from_utc_timestamp(F.current_timestamp(), "MST"))) #79200

    final_store = clean_response.select( 'email_send_id',
                                   'email_id',
                                   'email_message_guid',
                                   clean_response.response_mst_ts.alias('email_response_mst_ts'),
                                   clean_response.send_mst_ts.alias('email_send_mst_ts'),
                                   'email_send_mst_date',
                                   'email_response_type',
                                   'email_response_code',
                                   'content_block_id',
                                   'content_block_key_id',
                                   'content_block_key_name',
                                   'content_block_category_id',
                                   'content_block_category_name',
                                   'content_block_subcategory_id',
                                   'content_block_subcategory_name',
                                   'content_block_click_eid_text',
                                   'content_block_click_visit_guid',
                                   'content_block_click_visitor_guid',
                                   'first_open_flag',
                                   'first_click_flag',
                                   'first_complaint_flag',
                                   'template_id',
                                   'template_name',
                                   'email_control_flag',
                                   'email_language_code',
                                   'email_country_code',
                                   'private_label_id',
                                   'shopper_id',
                                   'shopper_country_code',
                                   'shopper_email_domain_name',
                                   'isc_source_code',
                                   'exclude_reason_desc',
                                   'response_url_text',
                                   'response_link_context_text',
                                   'etl_build_mst_ts',
                                   'campaign_id',
                                   'segment_id',
                                   'channel_id',
                                   'email_name',
                                   'response_link_name',
                                   'email_response_mst_date',
                                   'email_source_system_name') #79200


    final_store.repartition(5).write.mode("append").insertInto("enterprise.fact_marketing_email_response", overwrite = True)
