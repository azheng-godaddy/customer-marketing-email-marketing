import argparse
import sys
from datetime import date, datetime, timedelta
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, BooleanType, LongType
from pyspark.sql.functions import broadcast
import pytz



if __name__ == "__main__":


    spark = SparkSession \
        .builder\
        .appName("fact_email_marketing_email_response") \
        .config("spark.sql.broadcastTimeout","3600")\
        .config("spark.default.parallelism", "2000") \
        .config("spark.sql.shuffle.partitions", "2000") \
        .config("spark.memory.useLegacyMode", "false") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .config("spark.shuffle.consolidateFiles", "true") \
        .config("spark.port.maxRetries", "32") \
        .config("spark.rdd.compress", "true") \
        .config("spark.driver.maxResultSize","0")\
        .config("hive.exec.dynamic.partition", "true") \
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .config("spark.shuffle.service.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.driver.memoryOverhead","8192")\
        .config("spark.executor.memoryOverhead","8192") \
        .config("spark.network.timeout","600") \
        .config("spark.rpc.io.serverTreads","64") \
        .config("spark.speculation", "false") \
        .config("spark.sql.orc.filterPushdown", "true") \
        .config("spark.sql.caseSensitive", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
        .config("spark.sql.hive.metastorePartitionPruning", "true") \
        .enableHiveSupport() \
        .getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("--email_open_date", help="customer sent email open date")
    parser.add_argument("--email_open_end_date", help="customer sent email open end date")
    parser.add_argument("--email_queue_start_date", help="queue start date, it's 2017-07-17")
    parser.add_argument("--email_click_queue_start_date", help="1 year from the open date")
    parser.add_argument("--email_send_start_date", help="send start date, it's 2013-01-01")
    parser.add_argument("--email_end_date", help="30 days from open date")
    args = parser.parse_args()

    EMAIL_OPEN_DATE = args.email_open_date
    EMAIL_OPEN_END_DATE = args.email_open_end_date
    EMAIL_QUEUE_START_DATE =  args.email_queue_start_date #now - timedelta(days=366)
    EMAIL_CLICK_QUEUE_START_DATE = args.email_click_queue_start_date
    EMAIL_SEND_START_DATE = args.email_send_start_date
    EMAIL_END_DATE = args.email_end_date


    #import data into dataframe

    ###########################################  OCP  CLEAN LAYER ######################################
   
    df_ocp_contact_email= spark.sql("""select email_id,
                                        customermap_id,
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
                                        sent_mst_date from cust_contact_cln.ocp_contact_email_cln""")#73,529,133

    df_ocp_contact_open_email=spark.sql("""select email_open_id,
                                            open_mst_ts,
                                            customermap_id,
                                            shopper_id,
                                            shopper_email_domain_name,
                                            message_id,
                                            template_id,
                                            privatelabel_id,
                                            isc_code,
                                            language_code,
                                            open_mst_date from cust_contact_cln.ocp_contact_email_open_cln""") #2,853,104


    ############################################################## Response Data (OPEN, CLICK) #######################################################################

    
    #df_tes_template= spark.sql("select templateid, templatename,snap_date from emailsystem.tes_template_snap where snap_date='${snap_date}'")

    df_tes_template= spark.sql("select templateid, templatename from emailsystem.tes_template_snap ") #4,520
    

    
    # new tables for click
    df_email_success_tracking_click = spark.sql("""select trackingcode,
                                                          emailqueuesuccesstrackingclickid as content_block_id,
                                                             createdate,
                                                             visitguid as content_block_click_visit_guid,
                                                             visitorguid as content_block_click_visitor_guid,
                                                             eid as eid_text from emailsystem.oc_emailqueuesuccesstrackingclick_snap """)#167,200,672

    

    df_email_success_tracking_snap = spark.sql("""select emailqueuesuccessid, 
                                                            trackingcode, 
                                                            contentblockkeyid as content_block_key_id ,
                                                            emailqueuecreatedate
                                                            from emailsystem.oc_emailqueuesuccesstracking_snap """)#29,978,977,275  

    

    df_email_queue_success = spark.sql("select emailqueueid, emailqueuesuccessid, emailqueuecreatedate from emailsystem.oc_emailqueuesuccess_snap ")
#3,670,906,502
    

    df_email_queue_stage = spark.sql("""select messageguid,
                                             templateid,
                                             redpointcontactid
                                             from stage.marketing_email_response_queue_stage
                                             WHERE email_queue_create_date >= '{EMAIL_SEND_START_DATE}'
                                             AND email_queue_create_date <= '{EMAIL_END_DATE}' group by 1,2,3 """.format(EMAIL_SEND_START_DATE=EMAIL_SEND_START_DATE,EMAIL_END_DATE=EMAIL_END_DATE)).repartition(2000)  # Row count 3,388,248,161 Partitions: 1164
    #18,356,903

    de_dupe=['messageguid','templateid']
    df_email_queue_stage_clk = spark.sql("""select messageguid,
                                             emailqueueid,
                                             templateid,
                                             redpointcontactid,
                                             privatelabelid,
                                             email_queue_shopper_id,
                                             email_queue_create_date
                                             from stage.marketing_email_response_queue_stage
                                             WHERE email_queue_create_date >= '{EMAIL_SEND_START_DATE}'
                                             AND email_queue_create_date <= '{EMAIL_END_DATE}'  """.format(EMAIL_SEND_START_DATE=EMAIL_SEND_START_DATE,EMAIL_END_DATE=EMAIL_END_DATE)).dropDuplicates(de_dupe).repartition(2000)  # Row count 3,388,248,161 Partitions: 1164
                                           
#18,356,903
    ######################################################### CONTENT HIERARCHY #######################################################################


    df_email_content_blockkey = spark.sql("""select blockkeyname as content_block_key_name,
                                                        contentblockkeyid as content_block_key_id,
                                                        contentblocksubcategoryid as content_block_subcategory_id 
                                                        from emailsystem.oc_contentblockkey_snap """)

    #15,663

    df_email_content_block_subcategory = spark.sql("""select contentblockcategoryid as content_block_category_id,
                                                                   contentblocksubcategoryid as content_block_subcategory_id,
                                                                   blocksubcategoryname as content_block_subcategory_name
                                                                   from emailsystem.oc_contentblocksubcategory_snap """)
    
#770

    df_email_content_block_category = spark.sql("""select blockcategoryname as content_block_category_name,
                                                              contentblockcategoryid as content_block_category_id
                                                              from emailsystem.oc_contentblockcategory_snap """)
 
#37

    #dimension data
    
    df_fortknox_shopper = spark.sql("select shopper_id, UPPER(country) AS countrycode from fortknox.fortknox_shopper_snap ")

    #165,412,116
    

    #################################################### FILTER EMAIL SEND, OPEN, QUEUE ###############################################################################

    fil_ocp_contact_email = df_ocp_contact_email.filter(df_ocp_contact_email.sent_mst_date >= EMAIL_SEND_START_DATE ) \
                                        .filter(df_ocp_contact_email.sent_mst_date < EMAIL_END_DATE)#30,507,326
                                  

    fil_ocp_contact_open_email = df_ocp_contact_open_email \
                                    .filter(df_ocp_contact_open_email.open_mst_date >= EMAIL_OPEN_DATE) \
                                    .filter(df_ocp_contact_open_email.open_mst_date < EMAIL_OPEN_END_DATE) #2,853,104

    # filter the queue data, this is in future if we like to scale the
    fil_email_queue_stage = df_email_queue_stage #18,356,903




    ################################################################ CURRENT SEND + CUSTOMER DATA #####################################################

    
    # added max sent date to get only one record during the join
    join_cust_contact_send_date = fil_ocp_contact_email\
                                            .select('customermap_id',
                                                     'email_id',
                                                     'message_id',
                                                     'emailsourcekey',
                                                     'sent_mst_ts',
                                                     'template_id',
                                                     'emailsource_id',
                                                     'shopper_id',
                                                     'language_code',
                                                     'isc_code',
                                                     'privatelabel_id',
                                                     'shopper_email_domain_name'
                                                     ).repartition(200)  #30,507,326
                
    deduplicate_columns = ['shopper_id','template_id','message_id','isc_code']

    join_cust_contact_send = join_cust_contact_send_date.dropDuplicates(deduplicate_columns)#23,570,260


    #################################################### OPEN DATA ###############################################################################
    
    cond_send_response = [F.trim(F.lower(fil_ocp_contact_open_email.shopper_id)) == F.trim(F.lower(join_cust_contact_send.shopper_id)),
                        F.trim(F.lower(fil_ocp_contact_open_email.message_id)) == F.trim(F.lower(join_cust_contact_send.message_id)),
                        F.trim(F.lower(fil_ocp_contact_open_email.isc_code)) == F.trim(F.lower(join_cust_contact_send.isc_code)),
                        F.trim(F.lower(fil_ocp_contact_open_email.template_id)) == F.trim(F.lower(join_cust_contact_send.template_id))]

    cond_response_queue = [ F.trim(F.lower(fil_ocp_contact_open_email.message_id)) == F.trim(F.lower(fil_email_queue_stage.messageguid)),
                            F.trim(F.lower(fil_ocp_contact_open_email.template_id)) == F.trim(F.lower(fil_email_queue_stage.templateid))]


    join_email_open_send = broadcast(fil_ocp_contact_open_email) \
                                .join(join_cust_contact_send, cond_send_response, 'left') \
                                .drop(join_cust_contact_send.shopper_id) \
                                .drop(join_cust_contact_send.message_id) \
                                .drop(join_cust_contact_send.isc_code) \
                                .drop(join_cust_contact_send.template_id) \
                                .join(fil_email_queue_stage, cond_response_queue, 'left') \
                                .drop(fil_email_queue_stage.messageguid) \
                                .drop(fil_email_queue_stage.templateid) \
                                .select(fil_email_queue_stage.redpointcontactid,
                                        join_cust_contact_send.customermap_id,
                                        join_cust_contact_send.email_id,
                                        fil_ocp_contact_open_email.message_id,
                                        join_cust_contact_send.sent_mst_ts,
                                        join_cust_contact_send.emailsourcekey,
                                        fil_ocp_contact_open_email.template_id,
                                        join_cust_contact_send.emailsource_id,
                                        fil_ocp_contact_open_email.shopper_id,
                                        fil_ocp_contact_open_email.language_code,
                                        fil_ocp_contact_open_email.isc_code,
                                        fil_ocp_contact_open_email.privatelabel_id,
                                        join_cust_contact_send.shopper_email_domain_name,
                                        fil_ocp_contact_open_email.open_mst_ts.alias('email_response_mst_ts'),
                                        fil_ocp_contact_open_email.open_mst_date.alias('email_response_mst_date')
                                        ) \
                                        .withColumn('resp_cd', F.lit('o')) \
                                        .withColumn('resp_type', F.lit('open')) \
                                        .withColumn('content_block_id', F.lit(None).cast(StringType())) \
                                        .withColumn('content_block_click_visit_guid', F.lit(None).cast(StringType())) \
                                        .withColumn('content_block_click_visitor_guid', F.lit(None).cast(StringType())) \
                                        .withColumn('eid_text', F.lit(None).cast(StringType())) \
                                        .withColumn('content_block_key_id', F.lit(None).cast(StringType())) #2,853,106
    ########################################################### UNION CURRENT WITH HISTORIC #########################################################

    union_hist_current_response_open_data = join_email_open_send


    ################################################# CLICK DATA ############################################################################


    filter_df_email_success_tracking_click = df_email_success_tracking_click \
                                                        .filter(df_email_success_tracking_click.createdate >= EMAIL_OPEN_DATE) \
                                                        .filter(df_email_success_tracking_click.createdate <= EMAIL_OPEN_END_DATE)\
                                                        .filter(df_email_success_tracking_click.eid_text.startswith("ocp.email."))#159,412
                                                       

    filter_df_email_success_tracking_snap = df_email_success_tracking_snap \
                                        .filter(df_email_success_tracking_snap.emailqueuecreatedate >= EMAIL_QUEUE_START_DATE) \
                                        .filter(df_email_success_tracking_snap.emailqueuecreatedate <= EMAIL_END_DATE) #10,229,127  legacy:246,467,260


    filter_df_email_queue_success = df_email_queue_success \
                                                .filter(df_email_queue_success.emailqueuecreatedate >= EMAIL_QUEUE_START_DATE) \
                                                .filter(df_email_queue_success.emailqueuecreatedate <= EMAIL_END_DATE) #22,460,312  


    join_email_queue_success = filter_df_email_success_tracking_snap \
                                                .join(filter_df_email_success_tracking_click, 'trackingcode') \
                                                .drop(filter_df_email_success_tracking_click.trackingcode) \
                                                .join(filter_df_email_queue_success, 'emailqueuesuccessid') \
                                                .drop(filter_df_email_queue_success.emailqueuesuccessid) \
                                                .drop(filter_df_email_success_tracking_snap.emailqueuesuccessid) \
                                                .drop(filter_df_email_success_tracking_snap.trackingcode) \
                                                .select('emailqueueid',
                                                        'createdate',
                                                        'content_block_click_visit_guid',
                                                        'content_block_click_visitor_guid',
                                                        'eid_text',
                                                        'content_block_id',
                                                        'content_block_key_id') #1076  legacy: 137,392

    
    # union the old and new email content data
    union_old_new_email_success = join_email_queue_success


    ###################################################### CLICK DATA WITH SEND DATA ############################################################

    fil_union_current_send_data = join_cust_contact_send\
                                                .filter(join_cust_contact_send.sent_mst_ts >= EMAIL_CLICK_QUEUE_START_DATE) \
                                                .filter(join_cust_contact_send.sent_mst_ts <= EMAIL_END_DATE) #23,570,260


    fil_email_queue_stage_click = df_email_queue_stage_clk \
                                                .filter(df_email_queue_stage_clk.email_queue_create_date >= EMAIL_CLICK_QUEUE_START_DATE) \
                                                .filter(df_email_queue_stage_clk.email_queue_create_date <= EMAIL_END_DATE) #18,356,903

    cond_click_response = [F.trim(F.lower(fil_email_queue_stage_click.email_queue_shopper_id)) == F.trim(F.lower(fil_union_current_send_data.shopper_id)),
                           F.trim(F.lower(fil_email_queue_stage_click.messageguid)) == F.trim(F.lower(fil_union_current_send_data.message_id)),
                           F.trim(F.lower(fil_email_queue_stage_click.templateid)) == F.trim(F.lower(fil_union_current_send_data.template_id))]

    join_email_click_send = broadcast(union_old_new_email_success)\
                                            .join(fil_email_queue_stage_click, 'emailqueueid') \
                                            .drop(fil_email_queue_stage_click.emailqueueid) \
                                            .join(fil_union_current_send_data, cond_click_response) \
                                            .drop(fil_email_queue_stage_click.email_queue_shopper_id) \
                                            .drop(fil_email_queue_stage_click.messageguid) \
                                            .drop(fil_email_queue_stage_click.templateid) \
                                            .select(fil_email_queue_stage_click.redpointcontactid,
                                                    fil_union_current_send_data.customermap_id,
                                                    fil_union_current_send_data.email_id,
                                                    fil_union_current_send_data.message_id,
                                                    fil_union_current_send_data.sent_mst_ts,
                                                    fil_union_current_send_data.emailsourcekey,
                                                    fil_union_current_send_data.template_id,
                                                    fil_union_current_send_data.emailsource_id,
                                                    fil_union_current_send_data.shopper_id,
                                                    fil_union_current_send_data.language_code,
                                                    fil_union_current_send_data.isc_code,
                                                    fil_union_current_send_data.privatelabel_id,
                                                    fil_union_current_send_data.shopper_email_domain_name,
                                                    union_old_new_email_success.createdate.alias('email_response_mst_ts'),
                                                    F.date_format(union_old_new_email_success.createdate, 'yyyy-MM-dd').alias('email_response_mst_date'),
                                                    F.lit('c').alias('resp_cd'),
                                                    F.lit('click').alias('resp_type'),
                                                    union_old_new_email_success.content_block_id,
                                                    union_old_new_email_success.content_block_click_visit_guid,
                                                    union_old_new_email_success.content_block_click_visitor_guid,
                                                    union_old_new_email_success.eid_text,
                                                    union_old_new_email_success.content_block_key_id
                                                    ) #350 
    ###################################################### OPEN + CLICK DATA #####################################################################

    union_unified_click_open = union_hist_current_response_open_data.unionAll(join_email_click_send)


    ############################################################################ RED POINT ###############################################################


    # redpoint history columns are added
    find_redpoint = union_unified_click_open \
        .filter("redpointcontactid != 0 AND redpointcontactid is NOT NULL") \
        .drop('messageguid', 'templateid','privatelabelid')

    add_redpoint=find_redpoint\
                    .select('redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_ts',
                F.date_format(find_redpoint.sent_mst_ts, 'yyyy-MM-dd').alias('sent_mst_date'),
                'template_id',
                F.lit(9999).alias('emailsource_id'),
                'language_code',
                'isc_code',
                'privatelabel_id',   
                'email_response_mst_ts',
                'email_response_mst_date',
                'resp_cd',
                'resp_type',
                'content_block_click_visit_guid',
                'content_block_click_visitor_guid',
                'eid_text',
                'content_block_id',
                'content_block_key_id')\
        .withColumn('channel_name', F.lit(None).cast(StringType())) 
    ##################################################################### OCP ################################################################################

    #find OCP
    find_ocp = union_unified_click_open \
        .filter("redpointcontactid is NULL OR redpointcontactid = 0") \
        .drop('messageguid') \
        .drop('templateid', 'privatelabelid')\
        .select('redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_ts',
                F.date_format(union_unified_click_open.sent_mst_ts, 'yyyy-MM-dd').alias('sent_mst_date'),
                'template_id',
                'emailsource_id',
                'language_code',
                'isc_code',
                'privatelabel_id',
               'email_response_mst_ts',
               'email_response_mst_date',
               'resp_cd',
               'resp_type',
               'content_block_click_visit_guid',
               'content_block_click_visitor_guid',
               'eid_text',
               'content_block_id',
               'content_block_key_id'
                )\
        .withColumn('channel_name', F.lit(None).cast(StringType())) 


     ################################################################ CATEGORY COLUMNS #######################################################

    #union redpoint and ocp
    ocp_and_redpoint = add_redpoint.unionAll(find_ocp)


    ################################################################################# ADDITIONAL COLUMNS #############################################################
    #add additional fields
    ocp_redpoint_ext = ocp_and_redpoint \
        .select('redpointcontactid',
                'shopper_id',
                'shopper_email_domain_name',
                'email_id',
                'message_id',
                'sent_mst_date',
                'sent_mst_ts',
                ocp_and_redpoint.template_id.cast('string'),
                'language_code',
                'isc_code',
                'privatelabel_id',
                F.when(ocp_and_redpoint.channel_name.like('%control%'), 'true')
                .otherwise('false').alias('email_control_flag').cast('boolean'),
                F.when(ocp_and_redpoint.emailsource_id==8, 'massmailer')
                .when(ocp_and_redpoint.emailsource_id==9999, 'redpoint')
                .otherwise('ocp').alias('email_source_system_name'),
                'email_response_mst_ts',
                'email_response_mst_date',
                'resp_cd',
                'resp_type',
                'content_block_click_visit_guid',
                'content_block_click_visitor_guid',
                'eid_text',
                'content_block_id',
                'content_block_key_id'
                ).dropDuplicates()


    #join template columns, removed broadcast join here
    join_tes_template = ocp_redpoint_ext.join(df_tes_template, ocp_redpoint_ext.template_id==df_tes_template.templateid, 'left') \
                        .drop('templateid') \
                        .select(ocp_redpoint_ext.redpointcontactid.alias('email_source_lead_id'),
                                'shopper_id',
                                'shopper_email_domain_name',
                                'email_id',
                                ocp_redpoint_ext.message_id.alias('email_message_guid'),
                                ocp_redpoint_ext.sent_mst_ts.alias('email_send_mst_ts'),
                                ocp_redpoint_ext.sent_mst_date.alias('email_send_mst_date'),
                                'template_id',
                                df_tes_template.templatename.alias('template_name'),
                                F.split(ocp_redpoint_ext.language_code,'-').getItem(0).alias('email_language_code'),
                                F.split(ocp_redpoint_ext.language_code,'-').getItem(1).alias('email_country_code'),
                                F.lower(ocp_redpoint_ext.isc_code).alias('isc_source_code'),
                                ocp_redpoint_ext.privatelabel_id.alias('private_label_id'),
                                'email_source_system_name',
                                'email_response_mst_ts',
                                'email_response_mst_date',
                                'email_control_flag',
                                ocp_redpoint_ext.resp_cd.alias('email_response_code'),
                                ocp_redpoint_ext.resp_type.alias('email_response_type'),
                                'content_block_click_visit_guid',
                                'content_block_click_visitor_guid',
                                ocp_redpoint_ext.eid_text.alias('content_block_click_eid_text'),
                                'content_block_id',
                                'content_block_key_id'
                                ) \
                        .withColumn('email_send_id', F.sha2(F.concat_ws('|', 'email_message_guid','shopper_id','isc_source_code','template_id'), 256))


     ######################################################## ADD OTHER DIMENSIONS ##########################################################################

    ####################################################### CONTENT COLUMNS ##############################################################################
    #Add additional block content columns (left join to pull these columns) removing broadcast join here
    join_content_hierarchy = join_tes_template.join(df_email_content_blockkey, 'content_block_key_id', 'left') \
        .drop(df_email_content_blockkey.content_block_key_id) \
        .join(df_email_content_block_subcategory, 'content_block_subcategory_id', 'left') \
        .drop(df_email_content_block_subcategory.content_block_subcategory_id) \
        .join(df_email_content_block_category, 'content_block_category_id', 'left') \
        .drop(df_email_content_block_category.content_block_category_id)


    ######################################################## OTHER DIMENSIONS ( GEO, ISC, RESELLER) #####################################################################

    #join all the columns and rename the columns
    add_dims = join_content_hierarchy.join(df_fortknox_shopper, 'shopper_id', 'left').drop('country_code','eu_flag')\
    .withColumn('first_open_flag', F.lit(None).cast('boolean')) \
                        .withColumn('first_click_flag', F.lit(None).cast('boolean')) \
                        .withColumn('first_complaint_flag', F.lit(None).cast('boolean'))\
                        .withColumn('etl_build_mst_ts',(F.from_utc_timestamp(F.current_timestamp(), "MST")))\
                         .withColumn('exclude_reason_desc', F.lit(None).cast(StringType()))\
                         .withColumn('response_url_text', F.lit(None).cast(StringType()))\
                         .withColumn('campaign_id', F.lit(None).cast(IntegerType()))\
                         .withColumn('segment_id', F.lit(None).cast(IntegerType()))\
                         .withColumn('channel_id', F.lit(None).cast(IntegerType()))\
                         .withColumn('email_name', F.lit(None).cast(StringType()))

    
    final_store = add_dims.select( 'email_send_id',
                                   'email_id',
                                   'email_message_guid',
                                   'email_response_mst_ts',
                                   'email_send_mst_ts',
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
                                   add_dims.countrycode.alias('shopper_country_code'),
                                   'shopper_email_domain_name',
                                   'isc_source_code',
                                   'exclude_reason_desc',
                                   'response_url_text',
                                   add_dims.content_block_click_eid_text.alias('response_link_context_text'),
                                   'etl_build_mst_ts',
                                   'campaign_id',
                                   'segment_id',
                                   'channel_id',
                                   'email_name',
                                   add_dims.content_block_key_name.alias('response_link_name'),
                                   'email_response_mst_date',
                                   'email_source_system_name').distinct()

    final_store.repartition(5).write.insertInto("enterprise.fact_marketing_email_response", overwrite = True)
