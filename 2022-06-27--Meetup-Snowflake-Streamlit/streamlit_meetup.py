# Import required libraries
# Snowpark
from pickle import FALSE
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import avg, sum, col, call_udf, lit, call_builtin, year
# Pandas
import pandas as pd
#Streamlit
import altair as alt
import streamlit as st
from datetime import date
from datetime import datetime

#Set page context
st.set_page_config(
     page_title="Doordash Analytics",
     page_icon="🧊",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://developers.snowflake.com',
         'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Marketplace"
     }
 )

# Create Session object
def create_session_object():
    connection_parameters = {
   "account": "",
   "user": "",
   "password": "",
   "warehouse": "",
   "role": "accountadmin",
   "database": "",
   "schema": ""
   }
    session = Session.builder.configs(connection_parameters).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema()').collect())
    return session

def load_data(session): 
    ord_df = (session.table("W_DDW_ORDERS_F"))
    emp_df = (session.table("W_DDW_EMPLOYEESS_D"))
    mer_df = (session.table("W_DDW_MERCHANTS_D"))

    combined_df = (ord_df.join(emp_df, ord_df["K_EMPLOYEE_DLHK"] == emp_df["K_EMPLOYEE_DLHK"])
                            .join(mer_df, ord_df["K_MERCHANT_DLHK"] == mer_df["K_MERCHANT_DLHK"])
                            .select(ord_df["K_ORDER_DLHK"], emp_df["K_EMPLOYEE_DLHK"].alias("K_EMPLOYEE_DLHK"), emp_df["A_EMPLOYEE_NAME"], 
                                    mer_df["A_MERCHANT_NAME"], ord_df["A_DELIVERY_DATE"], ord_df["A_PICKUP_DELIVERY"], ord_df["M_SUBTOTAL"], 
                                    ord_df["M_TIP"], ord_df["M_SERVICE_FEE"], ord_df["M_ORDER_TOTAL"], ord_df["M_COMPANY_PAID"], ord_df["M_EMPLOYEE_PAID"], 
                                    ord_df["M_AMOUNT_ABOVE_BUDGET"], ord_df["IS_ABOVE_BUDGET"], ord_df["A_WEATHER_CONDITION_SUMMARY"])).collect()
    
    max_date = date.today()
    min_date = date(max_date.year-1, max_date.month, max_date.day)

    with st.container():
        values = st.slider(
            "Order Date Range",
            min_value=min_date,
            max_value=max_date,
            value=(min_date, max_date))

    pd_combined_df = pd.DataFrame(combined_df)

    # Filtering primary dataset by slider dates
    pd_combined_df["A_DELIVERY_DATE"] = pd.to_datetime(pd_combined_df["A_DELIVERY_DATE"]).dt.date
    pd_combined_df = pd_combined_df.loc[(pd_combined_df["A_DELIVERY_DATE"] >= values[0]) & (pd_combined_df["A_DELIVERY_DATE"] <= values[1])]

    # Creating column to distinguish month/year date
    pd_combined_df["MONTH"] = pd.DatetimeIndex(pd_combined_df["A_DELIVERY_DATE"]).month.map("{:02}".format).astype(str)
    pd_combined_df["YEAR"] = pd.DatetimeIndex(pd_combined_df["A_DELIVERY_DATE"]).year.astype(str)
    pd_combined_df["MONTH_YEAR"] = pd_combined_df["YEAR"] + '-' + pd_combined_df["MONTH"]

    order_ct = len(pd_combined_df.index)
    order_total = pd_combined_df["M_ORDER_TOTAL"].sum()
    employee_paid_total = pd_combined_df["M_EMPLOYEE_PAID"].sum()

    # Creating dataset that aggregates spend amounts by month/year date
    monthly_orders = pd_combined_df.groupby("MONTH_YEAR", as_index=False).agg({"M_ORDER_TOTAL": "sum",
                                                            "M_COMPANY_PAID": "sum",
                                                            "M_EMPLOYEE_PAID": "sum",
                                                            "M_SERVICE_FEE": "sum",
                                                            "M_TIP": "sum"})
    monthly_orders = pd.melt(monthly_orders, id_vars='MONTH_YEAR', value_vars=['M_ORDER_TOTAL', 'M_COMPANY_PAID', 'M_SERVICE_FEE', 'M_EMPLOYEE_PAID', 'M_TIP'], var_name='Spend Category')
    monthly_orders['value'] = monthly_orders['value'].astype(str)

    # Over budget dataset
    pd_over_budget = pd_combined_df.loc[(pd_combined_df["IS_ABOVE_BUDGET"] == True)]
    over_budget_ct = len(pd_over_budget)
    over_budget_avg = pd_over_budget['M_EMPLOYEE_PAID'].mean()

    # Aggregation by weather condition
    pd_weather = pd_combined_df.groupby("A_WEATHER_CONDITION_SUMMARY").size()

    # Aggregation by merchant
    pd_merchant = pd_combined_df.groupby("A_MERCHANT_NAME", as_index=False).agg({'M_ORDER_TOTAL': "sum"}).rename(columns={"M_ORDER_TOTAL":"TOTAL_AMOUNT"})
    print(pd_merchant)
    pd_merchant = pd_merchant.sort_values("TOTAL_AMOUNT", ascending=False).head(10)

    # Containerize metrics to present
    st.write('')
    col1, col2, col3, col4, col5 = st.columns(5)

    with st.container():
        col1.metric(label='Order Count', value=order_ct)
        col2.metric(label='Percentage of Orders Above Budget', value=f'{round(over_budget_ct/order_ct*100, 2):0.2f}%')
        col3.metric(label='Average Order Amount Over Budget', value=f'${round(over_budget_avg, 2):0.2f}')
        col4.metric(label='Percentage Paid by Employees', value=f'{round(employee_paid_total/order_total*100, 2):0.2f}%')
        col5.metric(label='Amount of All Orders', value=f'${round(order_total, 2):0.2f}')
    
    # Create line chart to show DDW monthly spend over time
    line_chart = alt.Chart(monthly_orders).mark_line().encode(
        x=alt.X('MONTH_YEAR', axis=alt.Axis(title='Date (Year-Month)')),
        y=alt.Y('value:Q', axis=alt.Axis(title='Dollar Amount ($)')),
        color='Spend Category'
    ).properties(
        width=1700,
        height=700
    )
    
    # Containerize bar charts
    with st.container():
        st.header('Order Spend by Month')
        st.altair_chart(line_chart)

    col1, col2 = st.columns(2)
    with col1:
        st.header('Count of Weather Conditions of Orders')
        st.bar_chart(pd_weather)

    bar_chart = alt.Chart(pd_merchant).mark_bar().encode(
        x=alt.X('TOTAL_AMOUNT:Q', axis=alt.Axis(title='Total Amount ($)')),
        y=alt.Y('A_MERCHANT_NAME', axis=alt.Axis(title='Merchant'))
    ).properties(
        width=850
    )
    with col2:
        st.header('Top Ten Merchants by Amount Spent')
        st.altair_chart(bar_chart)


if __name__ == "__main__":
    session = create_session_object()
    load_data(session)
