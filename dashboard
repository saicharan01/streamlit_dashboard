import pandas as pd
import streamlit as st
import snowflake.connector
from snowflake.snowpark import Session
from config import conn_params
import plotly.express as px
from streamlit_autorefresh import st_autorefresh


session = Session.builder.configs(conn_params).create()


st.title("Bronze & Silver Trends")
def refresh_count():
    df_bronze_count=session.sql("select count(1) AS BRONZE_RECORDS_COUNT from MAIN.VEHICLE_TELEMATICS_BRONZE").collect()
    df_silver_count=session.sql("select count(1)  AS SILVER_RECORDS_COUNT from MAIN.VEHICLE_TELEMATICS_SILVER").collect()
    df_gold_count=session.sql("select count(1) AS GOLD_RECORDS_COUNT from MAIN.VEHICLE_TELEMATICS_GOLD").collect()
    df_insurence_count=session.sql("select count(1) AS INSURANCE_RECORDS_COUNT from MAIN.INSURANCE_PREMIUM_PRICE").collect()
    columns = st.columns(3)

    with columns[0]:
        st.info(f'Bronze Records Count:\n```\n{df_bronze_count[0]["BRONZE_RECORDS_COUNT"]}\n```')

    with columns[1]:
        st.success(f'Silver Records Count:\n```\n{df_silver_count[0]["SILVER_RECORDS_COUNT"]}\n```')

    with columns[2]:
        st.warning(f'Gold Records Count:\n```\n{df_gold_count[0]["GOLD_RECORDS_COUNT"]}\n```')






def refresh_and_plot():
    df_silver=session.sql("""SELECT
                        CONCAT(HOUR(TO_TIMESTAMP(EVENT_WRITTEN_SNOWFLAKE_SILVER_TIMESTAMP)), ':', LPAD(MINUTE(TO_TIMESTAMP(EVENT_WRITTEN_SNOWFLAKE_SILVER_TIMESTAMP)), 2, '0')) AS TIME_INTERVAL
                        ,COUNT(*) AS EVENT_COUNT
                         FROM MAIN.VEHICLE_TELEMATICS_SILVER
                        GROUP BY TIME_INTERVAL
                        ORDER BY TIME_INTERVAL""").collect()
    sql_query = """
                WITH CTE AS (
                    SELECT
                        TO_TIMESTAMP(record_content:tripvehicledata[0]:PUBLISH_TIME::STRING) AS PUBLISH_TIME
                    FROM
                        MAIN.VEHICLE_TELEMATICS_BRONZE
                )

                SELECT
                    CONCAT(HOUR(PUBLISH_TIME), ':', LPAD(MINUTE(PUBLISH_TIME), 2, '0')) AS TIME_INTERVAL,
                    COUNT(*) AS EVENT_COUNT
                FROM CTE
                GROUP BY
                    TIME_INTERVAL
                ORDER BY
                    TIME_INTERVAL;
                """
    df_bronze=session.sql(sql_query).collect()

    columns = st.columns(2)
    df_bronze_pd = pd.DataFrame(df_bronze, columns=['TIME_INTERVAL', 'EVENT_COUNT'])
    fig_bronze = px.line(df_bronze_pd, x='TIME_INTERVAL', y='EVENT_COUNT', title='Count of Rows Over Time bronze')
    fig_bronze.update_layout(width=500, height=300) 

    columns[0].plotly_chart(fig_bronze)

    df_silver_pd = pd.DataFrame(df_silver, columns=['TIME_INTERVAL', 'EVENT_COUNT'])
    fig_silver = px.line(df_silver_pd, x='TIME_INTERVAL', y='EVENT_COUNT', title='Count of Rows Over Time silver')
    fig_silver.update_layout(width=500, height=300) 

    columns[1].plotly_chart(fig_silver)
        

st_autorefresh(interval=60000, key="data_refresher")

refresh_count()
refresh_and_plot()






