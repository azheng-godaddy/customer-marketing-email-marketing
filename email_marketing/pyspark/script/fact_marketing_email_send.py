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
        .appName("fact_marketing_email_send") \
        .config("spark.sql.broadcastTimeout","3600")\
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


    #import data into dataframe

    ###########################################  OCP  CLEAN LAYER ######################################

    df_ocp_contact_email= spark.sql("""select email_id, 
                                        message_id, 
                                        sent_mst_ts, 
                                        emailsourcekey, 
                                        template_id,
                                        emailsource_id, 
                                        language_code, 
                                        privatelabel_id, 
                                        shopper_id,
                                        shopper_email_domain_name,
                                        isc_code, 
                                        sent_mst_date from cust_contact_cln.ocp_contact_email_cln""")


    ###########################################  OCP EMAIL SYSTEM DATA ######################################

    df_emailqueuedetail = spark.sql("""select emailqueueid, 
                                            messageguid, 
                                            redpointcontactid, 
                                            isautomatedseedlist, 
                                            istestsend, 
                                            emailqueuecreatedate, 
                                            privatelabelid, 
                                            shopperid, 
                                            sourcecode from emailsystem.oc_emailqueuedetail_snap """)
    

#get the latest partition, need to be removed once the features which makes the table always points to latest partition 
    df_emailqueuesuccess = spark.sql('select emailqueueid, templateid, emailqueuecreatedate from emailsystem.oc_emailqueuesuccess_snap')


    df_tes_template= spark.sql('select templateid, templatename from emailsystem.tes_template_snap ')
    

    ########################################### REDPOINT DATA #########################################

    
    
    df_fortknox_shopper = spark.sql('select shopper_id, UPPER(country) AS countrycode from fortknox.fortknox_shopper_snap')

    
    #filter unwanted data 

    fil_ocp_contact_email = df_ocp_contact_email.filter(df_ocp_contact_email.sent_mst_date >= CUST_CONTACT_START_DATE).filter(df_ocp_contact_email.sent_mst_date < END_DATE)

    fil_emailqueuesuccess = df_emailqueuesuccess.filter(df_emailqueuesuccess.emailqueuecreatedate >=OC_START_DATE).filter(df_emailqueuesuccess.emailqueuecreatedate <= END_DATE)


    join_emailqueue = df_emailqueuedetail.filter \
        ((df_emailqueuedetail.isautomatedseedlist == 0) & (df_emailqueuedetail.istestsend == 0) & (df_emailqueuedetail.emailqueuecreatedate >=OC_START_DATE) & (df_emailqueuedetail.emailqueuecreatedate <= END_DATE)) \
        .join(fil_emailqueuesuccess, 'emailqueueid') \
        .drop(fil_emailqueuesuccess.emailqueueid) \
        .select('messageguid', 'privatelabelid', 'templateid', 'redpointcontactid')


    


    #find redpoint data

    cond = [fil_ocp_contact_email.message_id == join_emailqueue.messageguid, fil_ocp_contact_email.template_id == join_emailqueue.templateid]

    find_redpoint = join_emailqueue \
        .filter("redpointcontactid != 0 AND redpointcontactid is NOT NULL") \
        .join(fil_ocp_contact_email, cond) \
        .drop('messageguid', 'templateid','privatelabelid')

    add_redpoint=find_redpoint \
        .select('template_id',
                'redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_date',
                'sent_mst_ts',
                'emailsourcekey',
                F.lit(9999).alias('emailsource_id'),
                'language_code',
                'isc_code',
                'privatelabel_id').withColumn('channel_name', F.lit(None).cast(StringType())) 

    #find OCP

    find_ocp = join_emailqueue \
        .filter("redpointcontactid is NULL OR redpointcontactid = 0") \
        .join(fil_ocp_contact_email, cond) \
        .drop('messageguid') \
        .drop('templateid', 'privatelabelid') \
        .select('template_id',
               'redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_date',
                'sent_mst_ts',
                'emailsourcekey',
                'emailsource_id',
                'language_code',
                'isc_code',
                'privatelabel_id').withColumn('channel_name', F.lit(None).cast(StringType())) 

    
    

    #union redpoint and ocp
    ocp_and_redpoint = add_redpoint.union(find_ocp)

    #add additional fields
    ocp_redpoint_ext = ocp_and_redpoint \
        .select('redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_date',
                'sent_mst_ts',
                'emailsourcekey',
                ocp_and_redpoint.template_id.cast('string'),
                'emailsource_id',
                'language_code',
                'isc_code',
                'privatelabel_id',
                F.when(ocp_and_redpoint.emailsource_id==8, 'mass mailer')
                .when(ocp_and_redpoint.emailsource_id==9999, 'redpoint')
                .otherwise('ocp').alias('email_source_system_name'),
                F.when(ocp_and_redpoint.emailsource_id == 13, 'true')
                .otherwise('false').alias('email_holdout_flag').cast('boolean'),
                F.when(ocp_and_redpoint.channel_name.like('%control%'), 'true')
                .otherwise('false').alias('email_control_flag').cast('boolean'),
                F.when(ocp_and_redpoint.emailsource_id == 13, 'false')
                .otherwise('true').alias('email_sent_flag').cast('boolean')
                ).distinct()

    #generate PK
    join_tes_template = ocp_redpoint_ext.join(df_tes_template,ocp_redpoint_ext.template_id==df_tes_template.templateid, 'left_outer') \
        .drop('templateid')\
        .select(ocp_redpoint_ext.redpointcontactid.alias('email_source_lead_id'),
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                ocp_redpoint_ext.message_id.alias('email_message_guid'),
                'sent_mst_ts',
                ocp_redpoint_ext.sent_mst_date.alias('email_send_mst_date'),
                ocp_redpoint_ext.emailsourcekey.alias('email_source_key'),
                'template_id',
                df_tes_template.templatename.alias('template_name'),
                'emailsource_id',
                F.lower(ocp_redpoint_ext.isc_code).alias('isc_source_code'),
                ocp_redpoint_ext.privatelabel_id.alias('private_label_id'),
                'email_source_system_name',
                'language_code',
                'email_holdout_flag',
                'email_control_flag',
                'email_sent_flag')\
        .withColumn('email_language_code',F.split('language_code','-').getItem(0))\
        .withColumn('email_country_code',F.split('language_code','-').getItem(1))\
        .withColumn('email_send_id', F.sha2(F.concat_ws('|', 'email_message_guid','shopper_id','isc_source_code','template_id'), 256))

    add_dims = join_tes_template.join(df_fortknox_shopper, 'shopper_id', 'left').drop('country_code','eu_flag')\
    .withColumn('etl_build_mst_ts',(F.from_utc_timestamp(F.current_timestamp(), "MST")))\
    .withColumn('exclude_reason_desc', F.lit(None).cast(StringType()))\
    .withColumn('campaign_id', F.lit(None).cast(IntegerType()))\
    .withColumn('segment_id', F.lit(None).cast(IntegerType()))\
    .withColumn('channel_id', F.lit(None).cast(IntegerType()))\
    .withColumn('email_name', F.lit(None).cast(StringType()))


    final_store = add_dims.select('email_send_id',
                                  'email_id',
                                  'email_message_guid',
                                  add_dims.sent_mst_ts.alias('email_send_mts_ts'),
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
                                  add_dims.countrycode.alias('shopper_country_code'),
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
