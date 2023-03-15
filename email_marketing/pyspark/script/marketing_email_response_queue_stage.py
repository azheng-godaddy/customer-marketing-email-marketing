import argparse
import sys
from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, LongType
import pytz




if __name__ == "__main__":

    startTime = datetime.now()

    spark = SparkSession \
        .builder \
        .appName("fact_marketing_email_response_queue_stage") \
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

    parser.add_argument("--email_queue_create_date", help="marketing email queue create date")
    parser.add_argument("--email_end_date", help="end date from the email sent date, usually it's 30 days from email sent date")
    parser.add_argument('param', nargs=argparse.REMAINDER)
    args = parser.parse_args()

    EMAIL_QUEUE_CREATE_DATE = args.email_queue_create_date
    EMAIL_END_DATE = args.email_end_date

        

    df_email_queue_detail = spark.sql("""select emailqueueid, 
                                            messageguid, 
                                            redpointcontactid, 
                                            isautomatedseedlist, 
                                            istestsend, 
                                            emailqueuecreatedate, 
                                            privatelabelid, 
                                            shopperid,
                                            sourcecode from emailsystem.oc_emailqueuedetail_snap """)

    df_email_queue_success = spark.sql('select emailqueueid, templateid, emailqueuecreatedate from emailsystem.oc_emailqueuesuccess_snap')


    #filter unwanted data
    fil_email_queue_success = df_email_queue_success\
                                        .filter(df_email_queue_success.emailqueuecreatedate >= EMAIL_QUEUE_CREATE_DATE) \
                                        .filter(df_email_queue_success.emailqueuecreatedate <= EMAIL_END_DATE)


    #join email queue and success, this is to find the email queue source (redpoint/ocp)
    join_email_queue = df_email_queue_detail.filter((df_email_queue_detail.isautomatedseedlist == 0) & (df_email_queue_detail.istestsend == 0)
                                                    & (df_email_queue_detail.emailqueuecreatedate >= EMAIL_QUEUE_CREATE_DATE)
                                                    & (df_email_queue_detail.emailqueuecreatedate <= EMAIL_END_DATE)) \
                                            .join(fil_email_queue_success, 'emailqueueid') \
                                            .drop(fil_email_queue_success.emailqueueid) \
                                            .select('messageguid',
                                                    'emailqueueid',
                                                    'templateid',
                                                    'redpointcontactid',
                                                    'privatelabelid',
                                                    df_email_queue_detail.shopperid.alias('email_queue_shopper_id'),
                                                    df_email_queue_detail.sourcecode.alias('email_queue_source_code'),
                                                    df_email_queue_detail.emailqueuecreatedate.alias('emailqueuecreatedate_ts'),
                                                    F.date_format(df_email_queue_detail.emailqueuecreatedate, 'yyyy-MM-dd').alias('email_queue_create_date'))




    join_email_queue.repartition(5).write.mode("overwrite").insertInto("stage.marketing_email_response_queue_stage",overwrite = True)
