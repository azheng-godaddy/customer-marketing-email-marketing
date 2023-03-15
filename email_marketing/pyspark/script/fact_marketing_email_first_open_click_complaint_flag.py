import argparse
import sys
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, LongType
from pyspark.sql.functions import broadcast




if __name__ == "__main__":

    startTime = datetime.now()

    spark = SparkSession \
        .builder \
        .appName("fact_response_flag") \
        .config("spark.sql.broadcastTimeout","3600")\
        .config("spark.default.parallelism", "1000") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.memory.useLegacyMode", "false") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.port.maxRetries", "32") \
        .config("spark.executor.cores","5")\
        .config("spark.driver.cores","5")\
        .config("spark.rdd.compress", "true") \
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
    parser.add_argument("--email_start_date", help="start date of the partition that you want to update in uds response")
    parser.add_argument("--email_all_date", help="first date partition in the table")
    parser.add_argument("--email_end_date", help="end date of the partition that you want to update in uds response")
    parser.add_argument('param', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    EMAIL_START_DATE = args.email_start_date
    EMAIL_END_DATE = args.email_end_date
    EMAIL_ALL_DATE = args.email_all_date

   
    df_uds_response_all_start_date = spark.sql(""" select email_send_id,
                                                          email_response_type,
                                                          email_response_mst_ts,
                                                          email_response_mst_date,
                                                          email_source_system_name
                                                          from enterprise.fact_marketing_email_response""")


    df_uds_response_all_start_date_2 = spark.sql(""" select email_send_id,
                                                     email_id,
                                                     email_message_guid,
                                                     email_response_mst_ts,
                                                    email_send_mst_ts,
                                                    email_send_mst_date,
                                                    email_response_type,
                                                    email_response_code,
                                                    content_block_id,
                                                    content_block_key_id,
                                                    content_block_key_name,
                                                    content_block_category_id,
                                                    content_block_category_name,
                                                    content_block_subcategory_id,
                                                    content_block_subcategory_name,
                                                    content_block_click_eid_text,
                                                    content_block_click_visit_guid,
                                                    content_block_click_visitor_guid,
                                                    first_open_flag,
                                                    first_click_flag,
                                                    first_complaint_flag,
                                                    template_id,
                                                    template_name,
                                                    email_control_flag,
                                                    email_language_code,
                                                    email_country_code,
                                                    private_label_id,
                                                    shopper_id,
                                                    shopper_country_code,
                                                    shopper_email_domain_name,
                                                    isc_source_code,
                                                    exclude_reason_desc,
                                                    response_url_text,
                                                    response_link_context_text,
                                                    etl_build_mst_ts,
                                                    campaign_id,
                                                    segment_id,
                                                    channel_id,
                                                    email_name,
                                                    response_link_name,
                                                    email_response_mst_date,
                                                    email_source_system_name
                                                    from enterprise.fact_marketing_email_response""")

    fil_uds_response_all_start_date_2=df_uds_response_all_start_date_2.filter(df_uds_response_all_start_date_2.email_source_system_name.isin('sfmc')==False)

    df_uds_response_final  = spark.sql(""" select email_send_id,
                                                  email_response_type,
                                                  email_response_mst_ts,
                                                  email_response_mst_date,
                                                  email_source_system_name
                                                  from enterprise.fact_marketing_email_response""")

    fil_uds_response_all = df_uds_response_final \
                                    .filter(df_uds_response_final.email_response_mst_date >= EMAIL_ALL_DATE) \
                                    .filter(df_uds_response_final.email_response_mst_date <= EMAIL_END_DATE)\
                                    .filter(df_uds_response_final.email_source_system_name.isin('sfmc')==False)

    fil_uds_response_start_date = df_uds_response_all_start_date \
                                        .filter(df_uds_response_all_start_date.email_response_mst_date >= EMAIL_START_DATE) \
                                        .filter(df_uds_response_all_start_date.email_response_mst_date <= EMAIL_END_DATE)\
                                        .filter(df_uds_response_all_start_date.email_source_system_name.isin('sfmc')==False)




    group_min_ts_fil_uds_response_all = fil_uds_response_all.groupBy("email_send_id","email_response_type").agg(F.min("email_response_mst_ts").alias("email_response_mst_ts"))

    # inner join with the required records
    jc=[fil_uds_response_start_date.email_send_id == group_min_ts_fil_uds_response_all.email_send_id 
        , fil_uds_response_start_date.email_response_type == group_min_ts_fil_uds_response_all.email_response_type]
        
    join_uds_start_date_with_all =  group_min_ts_fil_uds_response_all \
                                            .join(fil_uds_response_start_date,jc,'right') \
                                            .filter(fil_uds_response_start_date.email_response_mst_ts > group_min_ts_fil_uds_response_all.email_response_mst_ts ) \
                                            .select(fil_uds_response_start_date.email_send_id,
                                                    fil_uds_response_start_date.email_response_type,
                                                    fil_uds_response_start_date.email_response_mst_date,
                                                    fil_uds_response_start_date.email_response_mst_ts) \
                                            .distinct()


    # condition to join back
    cond_response = [fil_uds_response_all_start_date_2.email_send_id == join_uds_start_date_with_all.email_send_id,
                     fil_uds_response_all_start_date_2.email_response_type == join_uds_start_date_with_all.email_response_type,
                     fil_uds_response_all_start_date_2.email_response_mst_ts == join_uds_start_date_with_all.email_response_mst_ts]


    # When first click is true then first open is also true even though open record doesn't exist
    join_uds_final_uds_start_date = fil_uds_response_all_start_date_2 \
                                            .filter(fil_uds_response_all_start_date_2.email_response_mst_date >= EMAIL_START_DATE ) \
                                            .filter(fil_uds_response_all_start_date_2.email_response_mst_date <= EMAIL_END_DATE ) \
                                            .join(broadcast(join_uds_start_date_with_all), cond_response, 'left' ) \
                                            .select( fil_uds_response_all_start_date_2.email_send_id,
                                                     'email_id',
                                                     'email_message_guid',
                                                     fil_uds_response_all_start_date_2.email_response_mst_ts,
                                                     'email_send_mst_ts',
                                                     'email_send_mst_date',
                                                     fil_uds_response_all_start_date_2.email_response_type,
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
                                                     F.when(fil_uds_response_all_start_date_2.email_response_code.isin('o') & join_uds_start_date_with_all.email_send_id.isNull(), True)
                                                     .otherwise(False).alias('first_open_flag'),
                                                     F.when(fil_uds_response_all_start_date_2.email_response_code.isin('c') & join_uds_start_date_with_all.email_send_id.isNull(), True)
                                                     .otherwise(False).alias('first_click_flag'),
                                                     F.when(fil_uds_response_all_start_date_2.email_response_code.isin('s') & join_uds_start_date_with_all.email_send_id.isNull(), True)
                                                     .otherwise(False).alias('first_complaint_flag'),
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
                                                     fil_uds_response_all_start_date_2.email_response_mst_date,
                                                     'email_source_system_name').distinct()

    join_uds_final_uds_start_date.repartition(5).write.mode("overwrite").insertInto("enterprise.fact_marketing_email_response",overwrite = True)
