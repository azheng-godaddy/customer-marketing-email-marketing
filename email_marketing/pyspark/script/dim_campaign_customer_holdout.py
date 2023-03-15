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
        .appName("dim_campaign_customer_holdout") \
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
    parser.add_argument("--year", help="siena snapshot year with format YYYY")
    parser.add_argument("--month", help="siena snapshot month ith format MM")
    parser.add_argument("--day", help="siena snapshot day ith format DD")
    args = parser.parse_args()
    
    YEAR=args.year
    MONTH=args.month
    DAY=args.day

    #load siena source
    df_campaign_holdout=spark.sql(""" select customer_id, 
                                             etl_build_utc_ts,
                                             cms_omni_channel_id as channel_id,
                                             locale_code as market_id,
                                             internal_source_code as isc_source_code,
                                             year,
                                             month,
                                             day
                                    from sfmc_campaign_cln.sfmc_campaign_holdout_cln """)\
                                .withColumn('etl_build_mst_ts', (F.from_utc_timestamp(F.current_timestamp(), "MST")))

    fill_campaign_holdout=df_campaign_holdout.filter(df_campaign_holdout.year==YEAR).filter(df_campaign_holdout.month==MONTH).filter(df_campaign_holdout.day==DAY)

    agg_campaign_holdout_fill= fill_campaign_holdout.withColumn('customer_shopper', fill_campaign_holdout.customer_id)\
    .withColumn('campaign_selection_mst_ts', (F.from_utc_timestamp(fill_campaign_holdout.etl_build_utc_ts, "MST"))).drop(fill_campaign_holdout.etl_build_utc_ts)


    
    # Get the max build time of customer_ids table. We need any build not necessarily the max
    max_bld = spark.sql('show partitions customers.customer_ids').collect()[-1]['partition'].split('=')[1]

    customer_view= '''
    select id as shopper_id, customerid from customers.customer_ids where build_ts = '{max_build_ts}'
    '''.format(max_build_ts=max_bld)

    df_customer= spark.sql(customer_view).distinct()
    
    agg_campaign_holdout=agg_campaign_holdout_fill.withColumn('snap_date', F.date_format(agg_campaign_holdout_fill.campaign_selection_mst_ts, 'yyyy-MM-dd'))
    
    join_customer= agg_campaign_holdout.join(df_customer,df_customer.customerid==agg_campaign_holdout.customer_shopper,'left').drop(df_customer.customerid)
#remove the dupes
    final_campaign_holdout= join_customer.select('customer_id',
                                        'campaign_selection_mst_ts',
                                        'channel_id',
                                        'shopper_id',
                                        'market_id',
                                        'isc_source_code',
                                        'etl_build_mst_ts',
                                        'snap_date').distinct()

    final_campaign_holdout.coalesce(5).write.mode("append").insertInto("enterprise.dim_campaign_customer_holdout", overwrite = True)
