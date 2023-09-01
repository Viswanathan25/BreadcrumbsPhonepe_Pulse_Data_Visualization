import pandas as pd
import os
import json
import psycopg2
import requests
import subprocess
import matplotlib.pyplot as plt
import plotly.express as px
import streamlit as st
from PIL import Image as PILImage
os.environ["GIT_PYTHON_REFRESH"] = "quiet"


project = psycopg2.connect(host='localhost', user='postgres', password='2532', database='phonepe')
cursor = project.cursor()


cursor.execute('''select * from aggregate_transaction;''')
project.commit()
t1 = cursor.fetchall()
df_aggregated_transaction = pd.DataFrame(t1, columns=['State', 'Year', 'Quarter', 'Transaction_type', 'Transaction_count', 'Transaction_amount'])

cursor.execute('''select * from aggregate_user;''')
project.commit()
t2 = cursor.fetchall()
df_aggregated_user = pd.DataFrame(t2, columns=['State', 'Year', 'Quarter', 'Brands', 'User_Count', 'User_Percentage'])

cursor.execute('''select * from map_transaction;''')
project.commit()
t3 = cursor.fetchall()
df_map_transaction = pd.DataFrame(t3, columns=['State', 'Year', 'Quarter', 'District', 'Transaction_Count', 'Transaction_Amount'])

cursor.execute('''select * from map_user;''')
project.commit()
t4 = cursor.fetchall()
df_map_user = pd.DataFrame(t4, columns=['State', 'Year', 'Quarter', 'District', 'Registered_User', 'Apps_Opened'])

cursor.execute('''select * from top_transaction;''')
project.commit()
t5 = cursor.fetchall()
df_top_transaction = pd.DataFrame(t5, columns=['State', 'Year', 'Quarter', 'District_Pincode', 'Transaction_count', 'Transaction_amount'])

cursor.execute('''select * from top_user;''')
project.commit()
t6 = cursor.fetchall()
df_top_user = pd.DataFrame(t6, columns=['State', 'Year', 'Quarter', 'District_PinCode', 'Registered_User'])

def map_amount_overall():
    a_t2 = df_aggregated_transaction[['State','Transaction_amount']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('-',' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.replace('Andaman & Nicobar Islands','Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.title()

    merge_df = df_state_names_tra.merge(a_t2, on='State')
    #merge_df.to_csv('State_trans.csv', index=False)
    #df_trans = pd.read_csv('State_trans.csv')

    trans_fig = px.choropleth(merge_df,
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_amount', color_continuous_scale='Sunsetdark' , range_color=(0,200000000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)

def map_amount(yr, q):
    year = int(yr)
    qr = int(q)
    a_t = df_aggregated_transaction[['State','Year','Quarter','Transaction_amount']]
    a_t1 = a_t.loc[(a_t['Year']==year) & (a_t['Quarter']==qr)]
    a_t2 = a_t1[['State','Transaction_amount']]
    a_t2 = a_t2.sort_values(by='State')
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature['properties']['ST_NM'] for feature in data1['features']]
    state_names_tra.sort()
    df_state_names_tra = pd.DataFrame({'State':state_names_tra})

    a_t2['State'] = a_t2['State'].str.replace('-',' ')
    a_t2['State'] = a_t2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
    a_t2['State'] = a_t2['State'].str.replace('Andaman & Nicobar Islands','Andaman & Nicobar')
    a_t2['State'] = a_t2['State'].str.title()

    merge_df = df_state_names_tra.merge(a_t2, on='State')
    #merge_df.to_csv('State_trans.csv', index=False)
    #df_trans = pd.read_csv('State_trans.csv')

    trans_fig = px.choropleth(merge_df,
                geojson = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                             featureidkey = 'properties.ST_NM', locations='State', color='Transaction_amount', color_continuous_scale='BuPu', range_color=(0, 200000000000))
    trans_fig.update_geos(fitbounds="locations", visible=False)
    #trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)

# OVERALL DATAFRAME

def df_overall():
    a_t = df_aggregated_transaction[['Transaction_type', 'Transaction_amount']]
    transaction_type = a_t.groupby('Transaction_type')['Transaction_amount'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)


# YEARLY DATAFRAME

def df(yr, q):
    year = int(yr)
    qr = int(q)
    a_t = df_aggregated_transaction[['Year', 'Quarter', 'Transaction_type', 'Transaction_amount']]
    a_t1 = a_t.loc[(a_t['Year'] == year) & (a_t['Quarter'] == qr)]
    a_t2 = a_t1[['Transaction_type', 'Transaction_amount']]
    transaction_type = a_t2.groupby('Transaction_type')['Transaction_amount'].sum()
    df1 = pd.DataFrame(transaction_type).reset_index()
    return st.dataframe(df1)


#-----------------------------------------------------------QUERIES-----------------------------------------------------------------------#

# (1) TOP 10 TRANSACTIONS

def one():
    ag1 = df_aggregated_transaction[['State','Transaction_amount']]
    ag1 = ag1.groupby('State')['Transaction_amount'].sum()
    ag1 = ag1.sort_values()

    agi = ag1.tail(10)
    fig = px.bar(agi, x=agi.index, y='Transaction_amount', color_discrete_sequence = px.colors.sequential.Rainbow)
    st.plotly_chart(fig)


# (2) LEAST 10 TRANSACTIONS

def two():
    ag1 = df_aggregated_transaction[['State', 'Transaction_amount']]
    ag1 = ag1.groupby('State')['Transaction_amount'].sum()
    ag1 = ag1.sort_values()

    agi = ag1.head(10)
    fig = px.bar(agi, x=agi.index, y='Transaction_amount', color_discrete_sequence=px.colors.sequential.Blues)
    st.plotly_chart(fig)


# (3) MOBILE BRAND vs TRANSACTION COUNT

def three():
    au=df_aggregated_user[['Brands','User_Count']]
    brand_transaction_counts = au.groupby('Brands')['User_Count'].sum()
    bt = pd.DataFrame(brand_transaction_counts).reset_index()
    fig2 = px.pie(bt, values='User_Count',
                 names='Brands',
                 color_discrete_sequence=px.colors.sequential.RdBu)
    st.plotly_chart(fig2)


# (4)TOP 10 APPS OPENED STATES

def four():
    mu1 = df_map_user[['State', 'Apps_Opened']]
    apps_opened = mu1.groupby('State')['Apps_Opened'].sum()
    ao = pd.DataFrame(apps_opened).reset_index()
    ao = ao.sort_values(by='Apps_Opened', ascending=False)  # Sort by 'Apps_Opened' column in descending order
    ao = ao.head(10)
    fig = px.bar(ao, x='State', y='Apps_Opened', color_discrete_sequence=px.colors.sequential.YlGn)
    st.plotly_chart(fig)


# (5) TOP 10 REGISTERED USERS

def five():
    mu = df_map_user[['State', 'Registered_User']]
    registered_user = mu.groupby('State')['Registered_User'].sum()
    ru = pd.DataFrame(registered_user).reset_index()
    ru = ru.sort_values(by='Registered_User', ascending=False)
    ru = ru.head(10)
    fig1 = px.bar(ru, x='State', y='Registered_User', color_discrete_sequence=px.colors.sequential.turbid)
    st.plotly_chart(fig1)


# (6) DISTRICT TRANSACTION

def six():
    mt = df_map_transaction[['District', 'Transaction_Amount']]
    district_transaction = mt.groupby('District')['Transaction_Amount'].sum()
    dt = pd.DataFrame(district_transaction).reset_index()
    dt = dt.sort_values(by='Transaction_Amount', ascending=False)
    dt = dt.head(10)
    fig2 = px.bar(dt, x='District', y='Transaction_Amount', color_discrete_sequence=px.colors.sequential.dense)
    st.plotly_chart(fig2)


# (7) DISTRICT vs REGISTERED USERS

def seven():
    mu = df_map_user[['State', 'Year', 'Quarter', 'District', 'Registered_User', 'Apps_Opened']]
    district_users = mu.groupby('District')['Registered_User'].sum()
    du = pd.DataFrame(district_users).reset_index()
    du = du.sort_values(by='Registered_User', ascending=False)
    du = du.head(10)
    fig3 = px.bar(du, x='District', y='Registered_User', color_discrete_sequence=px.colors.sequential.matter)
    st.plotly_chart(fig3)


# (8) TOP 10 APPS OPENED DISTRICTS

def eight():
    mu2 = df_map_user[['District', 'Apps_Opened']]
    apps_opened_district = mu2.groupby('District')['Apps_Opened'].sum()
    aod = pd.DataFrame(apps_opened_district).reset_index()
    aod = aod.sort_values(by='Apps_Opened', ascending=False)  # Sort by 'Apps_Opened' column in descending order
    aod = aod.head(10)
    fig8 = px.bar(aod, x='District', y='Apps_Opened', color_discrete_sequence=px.colors.sequential.Burg)
    st.plotly_chart(fig8)


# (9) LEAST 10 APPS OPENED DISTRICTS

def nine():
    mu3 = df_map_user[['District', 'Apps_Opened']]
    least_apps_opened_district = mu3.groupby('District')['Apps_Opened'].sum()
    aod1 = pd.DataFrame(least_apps_opened_district).reset_index()
    aod1 = aod1.sort_values(by='Apps_Opened')  # Sort by 'Apps_Opened' column in descending order
    aod1 = aod1.head(10)
    fig9 = px.bar(aod1, x='District', y='Apps_Opened', color_discrete_sequence=px.colors.sequential.Agsunset)
    st.plotly_chart(fig9)


st.set_page_config(layout="wide")  # Wide screen


def page1():
    st.title(":violet[PHONE PE PULSE DATA VISUALIZATION AND EXPLORATION]")
    col1, col2 = st.columns(2)
    with col1:
        image_path = r"D:\pho1.jpg"
        col1.image(PILImage.open(image_path), width=330)

        image_path = r"D:\pho2.jpg"
        col1.image(PILImage.open(image_path), width=330)

        image_path = r"D:\pho03.jpg"
        col1.image(PILImage.open(image_path), width=330)

        image_path = r"D:\pho04.jpg"
        col1.image(PILImage.open(image_path), width=330)

    with col2:
        st.caption("KEY **FEATURES AND SERVICES** OF PHONE PE :")
        st.write(
            "1. MONEY TRANSFER : PhonePe allows users to send and receive money instantly from their bank accounts. Users can also request money from friends and family.")
        st.write(
            "2. BILL PAYMENTS AND RECHARGES : Users can conveniently pay their utility bills, such as electricity, water, gas and more, directly from the app. They can also recharge mobile prepaid plans and DTH services.")
        st.write(
            "3. ONLINE AND OFFLINE PAYMENTS : PhonePe facilitates payments at both online and offline merchant establishments. Users can simply scan QR codes or use the 'Tap to Pay' feature for swift transactions.")
        st.write(
            "4. REWARDS AND OFFERS : PhonePe offers various cashback rewards, discounts, and exclusive deals to users who make transactions using the platform.")
        st.write(
            "5. SECURITY AND PRIVACY : PhonePe employs robust security measures to safeguard user data and financial information. It requires secure PIN or biometric authentication for transactions.")
        st.write(
            "6. USER-FRIENDLY INTERFACE : The app is designed with a user-friendly and intuitive interface, making it easy for people of all ages and backgrounds to navigate and perform transactions.")


def page2():
    col1, col2 = st.columns(2)
    with col1:
        tr_year = st.selectbox(':blue[**Select Year**]', ('Overall', '2018', '2019', '2020', '2021', '2022'))
    with col2:
        tr_quarter = st.selectbox(':blue[**Select Quarter**]', (1, 2, 3, 4))

    col1, col2 = st.columns(2)

    with col1:
        st.caption(":red[TRANSACTION TYPE VS TRANSACTION AMOUNT]")
        if tr_year == 'Overall':
            df_overall()
        else:
            df(tr_year, tr_quarter)

    with col2:
        st.caption('TRANSACTION AMOUNT')
        if tr_year == 'Overall':
            map_amount_overall()
        else:
            map_amount (tr_year, tr_quarter)


def page3():
    query = st.selectbox(":blue[Select Table]", ('None', 'Top 10 Transactions', 'Least 10 Transactions',
                                          'Mobile Brand vs Transaction Count', 'Top 10 Apps Opened States',
                                          'Top 10 Registered Users', 'Top 10 District Transaction',
                                          'District vs Registered Users', 'Top 10 Apps Opened Districts',
                                          'Least 10 Apps Opened Districts'))

    st.write(':violet[You selected]: ', query)

    if query == 'None':
        st.write("Select table")
    elif query == 'Top 10 Transactions':
        one()
    elif query == 'Least 10 Transactions':
        two()
    elif query == 'Mobile Brand vs Transaction Count':
        three()
    elif query == 'Top 10 Apps Opened States':
        four()
    elif query == 'Top 10 Registered Users':
        five()
    elif query == 'Top 10 District Transaction':
        six()
    elif query == 'District vs Registered Users':
        seven()
    elif query == 'Top 10 Apps Opened Districts':
        eight()
    elif query == 'Least 10 Apps Opened Districts':
        nine()


selected_page = st.sidebar.radio('VISIT:', ['Home', 'Explore', 'Analysis'])
if selected_page == 'Home':
    page1()
elif selected_page == 'Explore':
    page2()
elif selected_page == 'Analysis':
    page3()

#===============================================================THE END==================================================================#