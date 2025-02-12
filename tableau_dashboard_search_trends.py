"""
###
# Data Tier: Prod Table
# Tables: `proddb.public.search_vol_bucket_base_new`
#         `proddb.public.search_state_usage_rate_new`
#         `proddb.public.state_search_dashboard_new`
# Runbook:
# Design Doc:
# SLO:
# Description:
    This pipeline creates tables for tableau dashboard views for search team.
    ETL POC: eng-data-cx
    Dashboard POC: Viswanath Nara Raghupathi
    DS POC: Kevin Hill, Shyam Parikh

"""

from os.path import basename, splitext
from typing import Text
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from common_config.enums import AlertPriority
import datetime
from datetime import timedelta

from lib.operators import (
    DdSnowflakeTableSensor,
    DdSnowflakeSqlOperatorV2,
    DdSnowflakeValidationOperatorV2,
)

# Name of the DAG is derived from file Name, this is our standard to align DAG names with FileName for easier maintainence.
DAG_NAME = splitext(basename(__file__))[0]

# IN THE FUTURE, SOME OF THESE CAN POINT TO A CONSTANTS FILE
dag_args = {
    "owner": "eng-data-cx",
    "email": "eng-data-cx@doordash.com",
    "pager_duty_email": "de-consumer-oncall@doordash.pagerduty.com",
    "slack_webhook_path": "T03NG2JH1/B041SKKP4BD/Fo2gjN626iWH2k7IJwOy6dWZ",
    "retry_alert_priority": AlertPriority.LOW.value,
    "failure_alert_priority": AlertPriority.LOW.value,
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "data_tier": 1,
    "retry_delay": datetime.timedelta(minutes=10),
    "start_date": datetime.datetime(year=2023, month=11, day=30),
    "stage_database": "proddb",
    "stage_schema": "public",
    "target_database": "proddb",
    "target_schema": "public",
    "snowflake_warehouse": "etl_batch_de_cx_xxlarge",
}


dag = DAG(
    DAG_NAME,
    default_args=dag_args,
    max_active_runs=1,
    schedule_interval="0 16 * * *",
)
dag.doc_md = __doc__  # this is required and used for the metadata doc

# checking if upstream data exists
check_tables = [
    "public.dimension_query_to_vertical_mapping_and_item_score_analytics_dashboard",
]

check__ = {}

for tbl in check_tables:
    check__[tbl] = DdSnowflakeTableSensor(
        dag=dag,
        task_id=f"check__{tbl}",
        table_name=tbl,
        use_source=True,
        source_query=f"""
              SELECT
                CASE WHEN COUNT(1) > 0 THEN 0
                  ELSE -1
                END as return_code
              FROM
                {tbl}
              LIMIT 1
              """,
    )

build__search_vol_bucket_base_new = DdSnowflakeSqlOperatorV2(
    dag=dag,
    task_id=f"build__search_vol_bucket_base_new",
    query=[
        """
        CREATE OR REPLACE TABLE {TargetDatabase}.{TargetSchema}.search_vol_bucket_base_new AS
        WITH 
        search_base AS (
        SELECT 
          LOWER(TRIM(COALESCE(parent_term, search_query, search_term))) as search_term -- Dedup for dish narrowing feature on 03-15-2024
        , CASE 
            WHEN search_type = 'core_search' THEN 'store_search' 
            WHEN suggestion_type = 'cuisine_filter' THEN 'cuisine_filter'
          ELSE search_type END AS search_type -- Dedup for dish narrowing feature on 03-15-2024
        , dd_session_id || search_term || dd_device_id || search_type AS search_id
        , search_timestamp_pst
        , dd_device_id 
        , dd_session_id 
        , is_search_converted
        , first_converted_vertical_position AS first_converted_position
        , is_search_clicked
        , first_clicked_card_vertical_position AS first_clicked_position 
        , num_results_on_submission AS num_store_results
        , platform
        , dis.district_name AS district_name
        , search_origin AS origin
        , country_name
        , fs.submarket_name
        , fs.region_name
        , experience
        FROM edw.consumer.fact_search fs
        LEFT JOIN public.dimension_district as dis on dis.district_id = fs.district_id
        WHERE search_timestamp_pst_date >= '2023-11-01'
        AND search_type IN ('core_search', 'vertical_search_from_tab', 'cuisine_filter', 'cuisine_filter_from_browse_tab', 'vertical_search_from_landing', 'item_first_search')
          AND experience = 'doordash'
        
        UNION ALL -- append histoical data prior to 2023-11-01

        SELECT 
            search_term
        ,   search_type
        ,   search_id 
        ,   search_timestamp_pst
        ,   dd_device_id 
        ,   dd_session_id 
        ,   is_search_converted
        ,   first_converted_position
        ,   is_search_clicked
        ,   first_clicked_position
        ,   num_store_results
        ,   platform
        ,   district_name
        ,   origin
        ,   country_name
        ,   submarket_name
        ,   region_name
        ,   'doordash' AS experience
        FROM proddb.public.fact_core_search_metrics_archive
        WHERE search_timestamp_pst_date >= current_date - interval '2 years' 
            AND search_timestamp_pst_date < '2023-11-01'
            AND search_type = 'store_search'
        )
        , search_volume AS (
        SELECT
            s.search_term
            , APPROX_COUNT_DISTINCT(search_id) AS search_sessions
            , SUM(search_sessions) over(order by search_sessions desc) AS volume_cum_sum
        FROM search_base s
        LEFT JOIN
            public.dimension_query_to_vertical_mapping_and_item_score_analytics_dashboard i
        ON
            LOWER(s.search_term) = LOWER(i.search_term)
        GROUP BY 1
        )
        SELECT
            search_term
            ,search_sessions
            ,volume_cum_sum
            , CASE
                WHEN volume_cum_sum <= (SELECT max(volume_cum_sum)*0.57
                FROM search_volume)
                THEN 'head'
                WHEN volume_cum_sum  > (SELECT max(volume_cum_sum)*0.57 FROM search_volume)
                    AND volume_cum_sum  <= (SELECT max(volume_cum_sum)*0.8 FROM search_volume)
                THEN 'torso'
                WHEN volume_cum_sum  > (SELECT max(volume_cum_sum)*0.8 FROM search_volume)
                    AND volume_cum_sum  <= (SELECT max(volume_cum_sum)*0.9 FROM search_volume)
                THEN 'tail'
                WHEN volume_cum_sum  > (SELECT max(volume_cum_sum)*0.9 FROM search_volume)
                THEN 'long tail'
            END AS volume_bucket
        FROM
            search_volume
        """,
        "GRANT SELECT ON {TargetDatabase}.{TargetSchema}.search_vol_bucket_base_new TO ROLE read_only_users",
    ],
)

build__search_vol_bucket_base_new << [
    check__[
        "public.dimension_query_to_vertical_mapping_and_item_score_analytics_dashboard"
    ],
]

build__search_state_usage_rate_new = DdSnowflakeSqlOperatorV2(
    dag=dag,
    task_id=f"build__search_state_usage_rate_new",
    query=[
        """
        CREATE OR REPLACE TABLE {TargetDatabase}.{TargetSchema}.search_state_usage_rate_new AS
        WITH search_attributed_activity AS
        (
        SELECT 
            CONVERT_TIMEZONE('UTC','America/Los_Angeles',a.conversion_event_time)::date AS event_date, 
            a.dd_device_id, 
            a.conversion_event_session_id,
            nv.store_id AS new_vertical_store_id,
            a.product_feature, 
            a.conversion_event_properties:order_uuid::varchar AS order_uuid, 
            GOV*1.0/100 AS GMV
        FROM 
            edw.consumer.event_attribution a
            LEFT JOIN edw.finance.dimension_deliveries dd 
                ON a.conversion_event_properties:order_uuid::varchar = dd.order_cart_uuid
            LEFT JOIN proddb.tableau.new_verticals_stores nv
                ON a.store_id = nv.store_id
                AND nv.cng_business_line NOT IN ('Packages', 'Cannabis')
        WHERE 
            event_date >= '2023-11-01'
                AND a.conversion_event_label = 'checkout_success'
                AND a.product_feature IN ('Autocomplete','Core Search', 'Cuisine Filter', 'Vertical Search')
        )

        SELECT
            t1.event_date as dte
            , t1.platform
            , t1.DISTRICT_NAME
            , t1.SUBMARKET_NAME
            , t1.REGION_NAME
            , APPROX_COUNT_DISTINCT(
                t1.dd_device_id || t1.event_date
            ) AS daily_dd_visits
            , APPROX_COUNT_DISTINCT(
                t2.dd_device_id||t2.search_timestamp_pst::date
            ) AS visit_w_search_session
            , APPROX_COUNT_DISTINCT(
                t2.search_id
            ) AS search_sessions
            , APPROX_COUNT_DISTINCT(
                CASE WHEN t2.is_search_clicked=1 THEN t2.search_id END
            ) AS search_clicked_sessions
            , APPROX_COUNT_DISTINCT(
                CASE WHEN t2.IS_SEARCH_CONVERTED=1 THEN t2.search_id END
            ) AS search_converted_sessions
            , APPROX_COUNT_DISTINCT(
                CASE
                    WHEN t2.search_type = 'store_search'
                    AND COALESCE(t2.num_store_results, 0) = 0
                    THEN t2.search_id
                END
            ) AS search_null_sessions
            , SUM(DISTINCT
                CASE
                    WHEN t2.is_search_clicked = 1
                    THEN t2.first_clicked_position
                END
            ) AS first_clicked_position_sum
            , SUM(DISTINCT
                CASE
                    WHEN t2.is_search_converted = 1
                    THEN t2.first_converted_position
                END
            ) AS first_converted_position_sum
            , COUNT(DISTINCT t1.dd_device_id || t3.event_date) AS daily_search_order_devices
            , COUNT(DISTINCT t3.conversion_event_session_id || t3.dd_device_id || t3.new_vertical_store_id) AS nv_search_traffic
        FROM
            public.fact_unique_visitors_full_pt as t1
            
            LEFT JOIN
                search_base as t2
                ON t1.event_date = t2.search_timestamp_pst::date
                AND t1.dd_device_id = t2.dd_device_id

            LEFT JOIN search_attributed_activity as t3
                ON t1.event_date = t3.event_date
                AND t1.dd_device_id = t3.dd_device_id
        WHERE
            t1.event_date >= '2023-11-01'
        GROUP BY
            1,2,3,4,5

        UNION ALL 
        
        --append historical data prior to 2023-11-01
        SELECT 
           dte
        ,  platform
        ,  DISTRICT_NAME
        ,  SUBMARKET_NAME
        ,  REGION_NAME
        ,  daily_dd_visits
        ,  visit_w_search_session
        ,  search_sessions
        ,  search_clicked_sessions
        ,  search_converted_sessions
        ,  search_null_sessions
        ,  first_clicked_position_sum
        ,  first_converted_position_sum
        ,  daily_search_order_devices
        ,  nv_search_traffic
        FROM proddb.public.search_state_usage_rate_archive
        
        """,
        "GRANT SELECT ON {TargetDatabase}.{TargetSchema}.search_state_usage_rate_new TO ROLE read_only_users",
    ],
)

build__state_search_dashboard_new = DdSnowflakeSqlOperatorV2(
    dag=dag,
    task_id=f"build__state_search_dashboard_new",
    query=[
        """
        CREATE OR REPLACE TABLE {TargetDatabase}.{TargetSchema}.state_search_dashboard_new AS
        WITH search_base AS (
        SELECT 
          LOWER(TRIM(COALESCE(parent_term, search_query, search_term))) as search_term -- Dedup for dish narrowing feature on 03-15-2024
        , CASE 
            WHEN search_type = 'core_search' THEN 'store_search' 
            WHEN suggestion_type = 'cuisine_filter' THEN 'cuisine_filter'
          ELSE search_type END AS search_type
        , dd_session_id || search_term || dd_device_id || search_type AS search_id
        , search_timestamp_pst
        , dd_device_id 
        , dd_session_id 
        , is_search_converted
        , first_converted_vertical_position AS first_converted_position
        , is_search_clicked
        , first_clicked_card_vertical_position AS first_clicked_position 
        , num_results_on_submission AS num_store_results
        , platform
        , dis.district_name AS district_name
        , search_origin AS origin
        , country_name
        , fs.submarket_name
        , fs.region_name
        , experience
        , is_user_id_null
        , search_timestamp_pst_date
        FROM edw.consumer.fact_search fs
        LEFT JOIN public.dimension_district as dis on dis.district_id = fs.district_id
        WHERE search_timestamp_pst_date >= '2023-11-01'
        AND search_type IN ('core_search', 'vertical_search_from_tab', 'cuisine_filter', 'cuisine_filter_from_browse_tab', 'vertical_search_from_landing', 'item_first_search')
          AND experience = 'doordash'
        )
        
        SELECT
            t2.search_timestamp_pst::date AS dte
            , t2.platform
            , t2.district_name
            , t2.SEARCH_TYPE
            , t2.ORIGIN
            , t2.COUNTRY_NAME
            , t2.SUBMARKET_NAME
            , t2.REGION_NAME
            , t2.SEARCH_TERM
            , VB.volume_bucket
            , t2.IS_USER_ID_NULL
            , CASE
                WHEN t3.item_score > 0.7
                THEN 'item'
                ELSE 'store'
            END AS item_store_intention
            , CASE
                WHEN t3.rx_score > 0.005
                THEN 1
                ELSE 0
              END AS is_rx_intention
            , CASE
                WHEN (1-t3.rx_score) >= 0.005
                THEN 1
                ELSE 0
              END AS is_non_rx_intention
            , CASE
                WHEN t3.convenience_score >= 0.005
                THEN 1
                ELSE 0
              END AS is_conv_intention
            , CASE
                WHEN t3.grocery_score >= 0.005
                THEN 1
                ELSE 0
              END AS is_groc_intention
            , CASE
                WHEN t3.alcohol_score >= 0.005
                THEN 1
                ELSE 0
              END AS is_alc_intention
            , CASE
                WHEN t3.pet_score >= 0.005
                THEN 1
                ELSE 0
              END AS is_pet_intention
            , CASE
                WHEN t3.other_vertical_score >= 0.005
                THEN 1
                ELSE 0
              END AS is_other_vertical_intention,
            APPROX_COUNT_DISTINCT(t2.search_id) AS search_sessions,
            APPROX_COUNT_DISTINCT(
                CASE
                    WHEN t2.is_search_clicked = 1
                    THEN t2.search_id
                END
                ) AS search_clicked_sessions,
            APPROX_COUNT_DISTINCT(
                CASE
                    WHEN t2.IS_SEARCH_CONVERTED = 1
                    THEN t2.search_id
                END
                ) AS search_converted_sessions,
            APPROX_COUNT_DISTINCT(
                CASE
                    WHEN t2.search_type = 'store_search'
                    THEN search_id
                END
            ) AS store_search_sessions,
            APPROX_COUNT_DISTINCT(
                CASE
                    WHEN
                        t2.search_type = 'store_search'
                        AND COALESCE(t2.num_store_results, 0) = 0
                    THEN t2.search_id
                END
            ) AS search_null_sessions,
            SUM(DISTINCT
                CASE
                    WHEN t2.is_search_clicked = 1
                    THEN t2.first_clicked_position
                END
            ) AS first_clicked_position_sum,
            SUM(DISTINCT
                CASE
                    WHEN t2.is_search_converted = 1
                    THEN t2.first_converted_position
                END
            ) AS first_converted_position_sum

        FROM
            search_base AS t2
        LEFT JOIN
             {TargetDatabase}.{TargetSchema}.search_vol_bucket_base_new  vb
        ON
            LOWER(t2.search_term) = LOWER(vb.search_term)
        LEFT JOIN
            public.dimension_query_to_vertical_mapping_and_item_score_analytics_dashboard t3
        ON
            LOWER(t2.search_term) = LOWER(t3.search_term)
        WHERE
            t2.search_timestamp_pst_date >= '2023-11-01'
            -- and VB.volume_bucket in ('torso','head','tail')
        GROUP BY
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19

        UNION ALL  --append historical data prior to 2023-11-01

        SELECT 
          dte
        , platform
        , district_name
        , SEARCH_TYPE
        , ORIGIN
        , COUNTRY_NAME
        , SUBMARKET_NAME
        , REGION_NAME
        , SEARCH_TERM
        , volume_bucket
        , IS_USER_ID_NULL
        , item_store_intention
        , is_rx_intention
        , is_non_rx_intention
        , is_conv_intention
        , is_groc_intention
        , is_alc_intention
        , is_pet_intention
        , is_other_vertical_intention
        , search_sessions
        , search_clicked_sessions
        , search_converted_sessions
        , store_search_sessions
        , search_null_sessions
        , first_clicked_position_sum
        , first_converted_position_sum
        FROM proddb.public.state_search_dashboard_archive
        """,
        "GRANT SELECT ON {TargetDatabase}.{TargetSchema}.state_search_dashboard_new TO ROLE read_only_users",
    ],
)

build__state_search_dashboard_new << [
    build__search_vol_bucket_base_new,
]

end = DummyOperator(
    dag=dag,
    task_id="end",
)
end << [build__state_search_dashboard_new, build__search_state_usage_rate_new]