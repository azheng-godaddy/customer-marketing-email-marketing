'''
new martech sfmc pipelines are moving away from the "snapshot" model and going with an incremental model.
It's incremental and it does run everyday, but it's meta-data, so there isn't necessarily new data every day. 
each run will grab all metadata through all partitions, 
for the case, when for each email_id has multiple different email_name, it will always grab the lastest email_name 
'''
import sys
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
import argparse
from pyspark.sql import functions as F
from pyspark.sql.functions import col, row_number
from pyspark.sql.types import IntegerType, StringType, BooleanType
import pytz
from pyspark.sql.window import Window
from pyspark.sql.functions import desc



if __name__ == "__main__":


    spark = SparkSession \
        .builder \
        .appName("dim_email_template_metadata") \
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


    #load siena source
    df_email_template=spark.sql(""" select  email_template_id as email_id ,
                                            email_template_name as email_name,
                                            service_ticket_nr,
                                            from_utc_timestamp(create_utc_ts, 'MST') AS create_mst_ts
                                            from sfmc_email_events_cln.sfmc_email_template_cln """)\
                                .withColumn('etl_build_mst_ts', (F.from_utc_timestamp(F.current_timestamp(), "MST")))

    agg_email_template= df_email_template.withColumn('etl_build_mst_date', F.date_format(df_email_template.etl_build_mst_ts, 'yyyy-MM-dd'))


    de_dupe=['email_id','email_name']
    de_dupe_email_template= agg_email_template.select('email_id',
                                            'email_name',
                                            agg_email_template.service_ticket_nr.alias('service_ticket_number'),
                                            'create_mst_ts',
                                        'etl_build_mst_ts',
                                        'etl_build_mst_date').dropDuplicates(de_dupe)

    dedupe_email_id=de_dupe_email_template.withColumn('rn',row_number().over(Window.partitionBy('email_id').orderBy(desc("create_mst_ts"))))

    final_email_template=dedupe_email_id.filter(dedupe_email_id.rn==1).drop(dedupe_email_id.rn) #19,538,818 


    final_email_template.repartition(5).write.mode("append").insertInto("enterprise.dim_email_template_metadata", overwrite = True)
