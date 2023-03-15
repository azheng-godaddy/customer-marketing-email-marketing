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
        .appName("dim_campaign_metadata") \
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

    df_metadata_campaign_cln=spark.sql("""
    select  objective_name as objective,
            subobjective_name as subobjective,
            category_name as category,
            subcategory_name as subcategory,
            campaign_name,
            friendly_name,
            campaign_classification_name as campaign_classification,
            campaign_group_name as campaign_group,
            cms_omni_campaign_id,
            year,
            month,
            day from sfmc_metadata_cln.sfmc_metadata_campaign_cln 
    """)

    df_sfmc_metadata_channel_cln =spark.sql("""
    select  cms_omni_campaign_id,
            cms_omni_segment_id,
            cms_omni_channel_id,
            channel_name as channel,
            segment_channel_name,
            campaign_segment_channel_name,
            internal_source_code,
            email_optin_status_name as email_optIn_status,
            control_flag as control_ind,
            test_flag as test_ind,
            three_day_suppression_flag as three_day_suppression_ind,
            test_type_name as test_type,
            international_flag as international_ind,
            year,
            month,
            day from sfmc_metadata_cln.sfmc_metadata_channel_cln 
    """)

    df_sfmc_metadata_utm_cln =spark.sql("""
    select  product_name as product,
            product_content_name as product_content,
            revenue_flag as revenue_ind,
            cms_omni_channel_id,
            year,
            month,
            day from sfmc_metadata_cln.sfmc_metadata_utm_cln
    """)

    df_sfmc_metadata_segment_cln =spark.sql("""
    select  segment_name,
            campaign_segment_name,
            message_classification_name as message_classification,
            send_type_name as trigger_type,
            cms_omni_segment_id,
            year,
            month,
            day from sfmc_metadata_cln.sfmc_metadata_segment_cln
    """)


    cond=[df_sfmc_metadata_channel_cln.cms_omni_channel_id==df_sfmc_metadata_utm_cln.cms_omni_channel_id]
    cond1=[df_sfmc_metadata_channel_cln.cms_omni_segment_id==df_sfmc_metadata_segment_cln.cms_omni_segment_id]
    cond2=[df_sfmc_metadata_channel_cln.cms_omni_campaign_id==df_metadata_campaign_cln.cms_omni_campaign_id]

    agg_campaign_view_cln= df_sfmc_metadata_channel_cln.join(df_sfmc_metadata_utm_cln,cond,'inner').join(df_sfmc_metadata_segment_cln,cond1,'inner')\
        .join(df_metadata_campaign_cln,cond2,'inner').drop(df_sfmc_metadata_utm_cln.cms_omni_channel_id).drop(df_sfmc_metadata_segment_cln.cms_omni_segment_id)\
            .drop(df_metadata_campaign_cln.cms_omni_campaign_id).withColumn('etl_build_mst_ts', (F.from_utc_timestamp(F.current_timestamp(), "MST")))
    
    agg_campaign_view=agg_campaign_view_cln.withColumn('etl_build_mst_date', F.date_format(agg_campaign_view_cln.etl_build_mst_ts, 'yyyy-MM-dd'))\
        .withColumn('campaign_gho_filter_applied_flag', F.lit(None).cast('boolean'))
    
    final_store = agg_campaign_view.select( agg_campaign_view.cms_omni_campaign_id.alias('campaign_id'),
                                  agg_campaign_view.cms_omni_segment_id.alias('segment_id'),
                                  agg_campaign_view.cms_omni_channel_id.alias('channel_id'),
                                  agg_campaign_view.channel.alias('campaign_channel_name'),
                                  agg_campaign_view.internal_source_code.alias('campaign_internal_source_code'),
                                  agg_campaign_view.email_optIn_status.alias('campaign_optin_preference_desc'),
                                  agg_campaign_view.control_ind.alias('campaign_control_flag'),
                                  agg_campaign_view.test_ind.alias('campaign_test_flag'), 
                                  agg_campaign_view.three_day_suppression_ind.alias('campaign_three_day_suppression_flag'),
                                  agg_campaign_view.test_type.alias('campaign_test_type_desc'),
                                  agg_campaign_view.international_ind.alias('campaign_international_flag'),
                                  agg_campaign_view.product.alias('campaign_product_desc'),
                                  agg_campaign_view.product_content.alias('campaign_product_content_desc'),
                                  agg_campaign_view.revenue_ind.alias('campaign_revenue_generating_flag'),
                                  agg_campaign_view.objective.alias('campaign_objective_desc'),
                                  agg_campaign_view.subobjective.alias('campaign_subobjective_desc'),
                                  agg_campaign_view.category.alias('campaign_category_name'),
                                  agg_campaign_view.subcategory.alias('campaign_subcategory_name'),
                                  'campaign_name',
                                  agg_campaign_view.friendly_name.alias('campaign_friendly_name'),
                                  agg_campaign_view.campaign_classification.alias('campaign_classification_desc'),
                                  agg_campaign_view.campaign_group.alias('campaign_group_desc'),
                                  'segment_name',
                                  agg_campaign_view.message_classification.alias('campaign_message_classification'),
                                  agg_campaign_view.trigger_type.alias('campaign_trigger_type_desc'),
                                  'campaign_gho_filter_applied_flag',
                                  'etl_build_mst_ts',
                                  'etl_build_mst_date').distinct()         

    final_store.repartition(5).write.mode("append").insertInto("enterprise.dim_campaign_metadata", overwrite = True)
