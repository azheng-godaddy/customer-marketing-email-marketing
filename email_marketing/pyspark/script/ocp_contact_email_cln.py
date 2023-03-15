import os
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType
import pytz


if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("ocp_contact_email_cln") \
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

    parser.add_argument("--cust_contact_start_date", help="date to get data from cust_contact table")
    parser.add_argument("--oc_start_date", help="date to get data from oc emailsystem table. Date should -7 days than CUST_CONTACT_START_DATE")
    parser.add_argument("--end_date", help="current date which is end date")

    args = parser.parse_args()

    CUST_CONTACT_START_DATE= args.cust_contact_start_date
    OC_START_DATE=args.oc_start_date
    END_DATE=args.end_date

    #import data into dataframes
    df_customermap = spark.sql('select customermap_id, CASE WHEN LENGTH(shopperid) > 0 THEN shopperid ELSE "NULL" END as shopper_id, CASE WHEN LENGTH(emaildomain) > 0 THEN emaildomain ELSE "NULL" END as shopper_email_domain_name from cust_contact.customermap_snap')


    df_contact_email = spark.sql("""select customermap_id,
                                            email_id, 
                                            message_id, 
                                            sentdate AS sent_mst_ts,
                                            emailsourcekey, 
                                            template_id, 
                                            emailsource_id, 
                                            lang as language_code, 
                                            CASE WHEN LENGTH(isccode) > 0 THEN isccode ELSE "NULL" END as isc_code,
                                            privatelabel_id 
                                            from cust_contact.contact_email_snap """)

   #cleanse the data for clean layer
    
    fil_contact_email = df_contact_email.filter(df_contact_email.sent_mst_ts >= CUST_CONTACT_START_DATE )\
    .filter(df_contact_email.sent_mst_ts <= END_DATE ).filter(df_contact_email.emailsource_id.isin(7,8,13))

    join_cust_contact_send_date = fil_contact_email.join(df_customermap, 'customermap_id') \
                                        .drop(df_customermap.customermap_id)\
                                        .distinct() 

    tz_contact_email= join_cust_contact_send_date.withColumn("sent_mst_date",F.date_format("sent_mst_ts",format='yyyy-MM-dd'))

    

    final_stage=tz_contact_email.withColumn('etl_build_mst_ts',(F.from_utc_timestamp(F.current_timestamp(), "MST")))

    final_store=final_stage.select('email_id',
                                    'sent_mst_ts',
                                    'customermap_id',
                                    'shopper_id',
                                    'shopper_email_domain_name',
                                    'privatelabel_id',
                                    'isc_code',
                                    'emailsource_id',
                                    'emailsourcekey',
                                    'template_id',
                                    'message_id',
                                    'language_code',
                                    'etl_build_mst_ts',
                                    'sent_mst_date'
                                    )


    final_store.repartition(5).write.insertInto("cust_contact_cln.ocp_contact_email_cln", overwrite = True)
