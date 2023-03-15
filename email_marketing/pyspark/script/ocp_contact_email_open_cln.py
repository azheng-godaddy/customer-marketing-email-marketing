import sys
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType
import pytz




if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("ocp_contact_email_open_cln") \
        .config("spark.default.parallelism", "1000") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.memory.useLegacyMode", "false") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.port.maxRetries", "32") \
        .config("spark.rdd.compress", "true") \
        .config("spark.executor.cores","5")\
        .config("spark.driver.cores","5")\
        .config("hive.exec.dynamic.partition", "true") \
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
    parser.add_argument("--email_open_date", help="customer sent email open date")
    parser.add_argument("--email_open_end_date", help="customer sent email open end date")


    args = parser.parse_args()

    EMAIL_OPEN_DATE=args.email_open_date

    EMAIL_OPEN_END_DATE=args.email_open_end_date


    #import data into dataframes
    df_customermap = spark.sql('select customermap_id, CASE WHEN LENGTH(shopperid) > 0 THEN shopperid ELSE "NULL" END as shopper_id, CASE WHEN LENGTH(emaildomain) > 0 THEN emaildomain ELSE "NULL" END as shopper_email_domain_name from cust_contact.customermap_snap')

    df_contact_email_open = spark.sql("""select email_open_id,
                                          message_id,
                                          CASE WHEN LENGTH(isccode) > 0 THEN isccode ELSE "NULL" END as isc_code,
                                          template_id,
                                          opendate as open_mst_ts,
                                          lang as language_code,
                                          privatelabel_id,
                                          customermap_id
                                          from cust_contact.contact_email_open_snap """)

    fil_contact_email_open = df_contact_email_open \
                                    .filter(df_contact_email_open.open_mst_ts >= EMAIL_OPEN_DATE) \
                                    .filter(df_contact_email_open.open_mst_ts <= EMAIL_OPEN_END_DATE) 

    join_cust_contact_open_date = fil_contact_email_open  \
                                        .join(df_customermap, 'customermap_id') \
                                        .drop(df_customermap.customermap_id)\
                                        .distinct()

    tz_contact_email= join_cust_contact_open_date.withColumn("open_mst_date",F.date_format("open_mst_ts",format='yyyy-MM-dd'))

    final_stage=tz_contact_email.withColumn('etl_build_mst_ts',(F.from_utc_timestamp(F.current_timestamp(), "MST")))

    final_store=final_stage.select('email_open_id',
                                    'open_mst_ts',
                                    'customermap_id',
                                    'shopper_id',
                                    'shopper_email_domain_name',
                                    'message_id',
                                    'template_id',
                                    'privatelabel_id',
                                    'isc_code',
                                    'language_code',
                                    'etl_build_mst_ts',
                                    'open_mst_date')


    final_store.repartition(5).write.insertInto("cust_contact_cln.ocp_contact_email_open_cln", overwrite = True)
