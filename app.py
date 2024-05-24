import streamlit as st
import pandas as pd
import utils
from time import time
import json
import uuid
from datetime import datetime
from io import BytesIO
from random import randint
from time import sleep
import boto3
from io import BytesIO
from zipfile import ZipFile

st.set_page_config(page_title='Invoice Processor', 
                   page_icon=None, layout="wide", 
                   initial_sidebar_state="auto", 
                   menu_items=None)

st.title(':orange[Trump vs Biden: TV Ads Analysis]')

s3_client = boto3.client('s3',
            aws_access_key_id = utils.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = utils.AWS_SECRET_KEY)


BUCKET = utils.BUCKET

if 'search_string' not in st.session_state:
    st.session_state['search_string'] = ''

# select which capmaign to show: Trump or Biden
campaign_name = st.sidebar.radio('select candidate',('Biden','Trump')).lower()


files_df, last_update = utils.load_invoice_df(s3_client, campaign_name, bucket = BUCKET, counter=None)
min_date = files_df['create_ts'].tolist()[-1]
start_date = st.sidebar.date_input("Select start_date", 
                                   min_value=datetime.date(min_date),
                                   max_value=datetime.now(),
                                   value=datetime.date(min_date))
start_date = pd.to_datetime(start_date,utc=True)

col1, col2 = st.columns(2)
col1.success(f"Last update: {last_update}")
col2.success(f"Total number of files: {files_df.shape[0]}")

search_string = st.sidebar.text_input('search text', value = st.session_state['search_string'])
st.session_state['search_string'] = search_string
if st.sidebar.button('clear search box'):
    st.session_state['search_string']=''

if st.session_state['search_string'] !='':
    try:
        search_string = st.session_state['search_string']
        files_df = files_df.query("search_col.str.contains(@search_string, case=False)")
    except:
        st.error("failed to find file which contains search string")





default_cols=['index','file_name','create_ts','completion']
selection = utils.dataframe_with_selections(files_df[default_cols],
                                            start_date=start_date)
# st.write(files_df.columns)



if st.button(":blue[Show selected files]"):
    if selection.empty:
        st.error("You have not selected any files")
        st.stop()

    else:

        df_to_show = files_df.loc[selection.index,:].copy()
        st.dataframe(df_to_show[default_cols])

        pil_images_to_show, byte_images_to_show = utils.get_images_to_show(s3_client,df_to_show)

        for row in df_to_show.itertuples():
                    # st.session_state['summary_df'][row[0]] = pd.DataFrame()
            invoice_images = pil_images_to_show[row.file_name]
            byte_invoice_images = byte_images_to_show[row.file_name]                                       


            with st.expander(f":green[Show invoice pages:] {row.file_name}"):

                btn_invoice = utils.download_invoice_as_image(row.file_name, byte_invoice_images)
                tab_names = [f"page {i+1}" for i in range(len(invoice_images))]
                
                for ind, page_tab in enumerate(st.tabs(tab_names)):
                    page_tab.image(invoice_images[ind])
                    fn=f"{row.file_name}_page_{ind}.jpg"
                    img_byte_arr = byte_invoice_images[ind]


                    btn_page = utils.download_image(fn, img_byte_arr, ind,page_tab)
                

st.write(files_df.query("completion.notna()").shape[0])

