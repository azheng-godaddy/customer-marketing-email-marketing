import sys
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType
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


if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("fact_marketing_email_send_siena") \
        .config("spark.default.parallelism", "1000") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.memory.useLegacyMode", "false") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.port.maxRetries", "32") \
        .config("spark.rdd.compress", "true") \
        .config("hive.exec.dynamic.partition", "true") \
        .config("spark.executor.cores","5")\
        .config("spark.driver.cores","5")\
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.speculation", "false") \
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    parser = argparse.ArgumentParser()

    parser.add_argument("--cust_contact_start_date", help="date to get data from cust_contact table")
    parser.add_argument("--oc_start_date", help="date to get data from oc emailsystem table. Date should -7 days than CUST_CONTACT_START_DATE")
    parser.add_argument("--end_date", help="current date which is end date")
    parser.add_argument("--year", help="siena snapshot year with format YYYY")
    parser.add_argument("--month", help="siena snapshot month ith format MM")
    parser.add_argument("--day", help="siena snapshot day ith format DD")
    args = parser.parse_args()

    CUST_CONTACT_START_DATE= args.cust_contact_start_date
    OC_START_DATE=args.oc_start_date
    END_DATE=args.end_date
    YEAR=args.year
    MONTH=args.month
    DAY=args.day
    SEND_YEAR= CUST_CONTACT_START_DATE[0:4]


# get a list of internal customer_ids from csv file
    df_blacklist= spark.read.csv('s3://gd-ckpetlbatch-prod-code/enterprise_data/email_marketing/sfmc_blacklist.csv', header = True)

#load siena source
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
    

    df_fortknox_shopper = spark.sql('select shopper_id, UPPER(country) AS countrycode from fortknox.fortknox_shopper_snap')
    
    # Get the max build time of customer_ids table. We need any build not necessarily the max
    max_bld = spark.sql('show partitions customers.customer_ids').collect()[-1]['partition'].split('=')[1]

    customer_view= '''
    select id, customerid from customers.customer_ids where build_ts = '{max_build_ts}'
    '''.format(max_build_ts=max_bld)

    df_customer= spark.sql(customer_view).distinct()

# filter sinea source based on date 

    fill_events_sent_log= df_events_sent_log.withColumn('send_mst_ts', (F.from_utc_timestamp(df_events_sent_log.event_utc_ts, "MST"))).drop(df_events_sent_log.event_utc_ts)

    filter_events_sent_log= fill_events_sent_log.filter(fill_events_sent_log.send_mst_ts >= CUST_CONTACT_START_DATE).filter(fill_events_sent_log.send_mst_ts <= END_DATE)


    fill_events_sent= df_events_sent.withColumn('send_mst_ts', (F.from_utc_timestamp(df_events_sent.event_utc_ts, "MST"))).drop(df_events_sent.event_utc_ts)


    filter_events_sent= fill_events_sent.filter(fill_events_sent.send_mst_ts >= CUST_CONTACT_START_DATE).filter(fill_events_sent.send_mst_ts <= END_DATE)


#join siena send and send-log 
    cond= [filter_events_sent.sfmc_job_id==filter_events_sent_log.sfmc_job_id,filter_events_sent.sfmc_list_id==filter_events_sent_log.sfmc_list_id,
          filter_events_sent.sfmc_batch_id==filter_events_sent_log.sfmc_batch_id,
          filter_events_sent.sfmc_subscriber_id==filter_events_sent_log.sfmc_subscriber_id]

    join_send_log= filter_events_sent.join(filter_events_sent_log,cond,'left').drop(filter_events_sent_log.send_mst_ts)\
    .drop(filter_events_sent_log.sfmc_subscriber_id).drop(filter_events_sent_log.sfmc_list_id).drop(filter_events_sent_log.sfmc_batch_id)\
    .drop(filter_events_sent_log.sfmc_job_id)

    # take the advantage of the exclude_reason_desc col, if the customer_id is interal, exclude_reason_desc is internal, otherwise it will be null
    exclude_col= F.when(df_blacklist.QaCustomerID.isNull(), F.lit(None).cast(StringType())).otherwise('internal customer')

    exclude_black_list = join_send_log.join(df_blacklist, join_send_log.customer_id==df_blacklist.QaCustomerID, 'left')\
    .withColumn('exclude_reason_desc',exclude_col)

    join_customer= exclude_black_list.join(df_customer,df_customer.customerid==exclude_black_list.customer_id,'left').drop(df_customer.customerid)


    clean_send= join_customer.join(df_fortknox_shopper, df_fortknox_shopper.shopper_id==join_customer.id, 'left')\
                                    .select('send_mst_ts',
                                     join_customer.email_id.alias('template_id'),
                                     join_customer.internal_source_code.alias('isc_source_code'),
                                     join_customer.id.alias('shopper_id'),
                                     df_fortknox_shopper.countrycode.alias('shopper_country_code'),
                                     'sfmc_job_id',
                                     'send_mst_ts',
                                     'sfmc_list_id',
                                     'sfmc_batch_id',
                                     'sfmc_subscriber_id',
                                     'campaign_id',
                                     'segment_id',
                                     'channel_id',
                                     'email_name',
                                     'exclude_reason_desc',
                                     'private_label_id',
                                     F.split(join_customer.locale_code,'-').getItem(0).alias('email_language_code'),
                                     F.split(join_customer.locale_code,'-').getItem(1).alias('email_country_code')
                                     )\
    .withColumn('email_send_id', F.sha2(F.concat_ws('|', 'sfmc_job_id','sfmc_list_id','sfmc_batch_id','sfmc_subscriber_id'), 256))\
    .withColumn('email_id',F.lit(None).cast(IntegerType()))\
    .withColumn('email_message_guid',F.lit(None).cast(StringType()))\
    .withColumn('shopper_email_domain_name',F.lit(None).cast(StringType()))\
    .withColumn('template_name',F.lit(None).cast(StringType()))\
    .withColumn('email_source_lead_id',F.lit(None).cast(IntegerType()))\
    .withColumn('email_source_key',F.lit(None).cast(IntegerType()))\
    .withColumn('email_holdout_flag',F.lit(None).cast('boolean'))\
    .withColumn('email_control_flag',F.lit(None).cast('boolean'))\
    .withColumn('email_sent_flag',F.lit(True).cast('boolean'))\
    .withColumn('email_send_mst_date',F.date_format(join_customer.send_mst_ts, 'yyyy-MM-dd'))\
    .withColumn('email_source_system_name',F.lit('sfmc').cast(StringType()))\
    .withColumn('etl_build_mst_ts', (F.from_utc_timestamp(F.current_timestamp(), "MST")))


    final_store = clean_send.select('email_send_id',
                                  'email_id',
                                  'email_message_guid',
                                  clean_send.send_mst_ts.alias('email_send_mst_ts'),
                                  'email_source_lead_id',
                                  'email_source_key',
                                  'template_id',
                                  'template_name', 
                                  'email_holdout_flag',
                                  'email_control_flag',
                                  'email_sent_flag',
                                  'email_language_code',
                                  'email_country_code',
                                  'private_label_id',
                                  'shopper_id',
                                  'shopper_country_code',
                                  'shopper_email_domain_name',
                                  'isc_source_code',
                                  'exclude_reason_desc',
                                  'etl_build_mst_ts',
                                  'campaign_id',
                                  'segment_id',
                                  'channel_id',
                                  'email_name',
                                  'email_send_mst_date',
                                  'email_source_system_name').distinct()


    final_store.repartition(5).write.mode("append").insertInto("enterprise.fact_marketing_email_send", overwrite = True)
