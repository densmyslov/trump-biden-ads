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

st.set_page_config(page_title='PDF/Image Data Retriever', 
                   page_icon=None, layout="wide", 
                   initial_sidebar_state="auto", 
                   menu_items=None)

st.title(':orange[Trump vs Biden: TV Ads Analysis]')

st.write(utils.AWS_ACCESS_KEY_ID)

s3_client = boto3.client('s3',
            aws_access_key_id = utils.AWS_ACCESS_KEY_ID,
            aws_secret_access_key = utils.AWS_SECRET_KEY)


BUCKET = utils.BUCKET

tab_view_dataset, tab_zero_shot =  st.tabs(('View dataset','Wrong predictions by GPT-4o'))

# Initialize session state for view
if 'view' not in st.session_state:
    st.session_state.view = None

def set_view(view):
    st.session_state.view = view

with tab_view_dataset:
    set_view('dataset')
    
    
    if 'search_string' not in st.session_state:
        st.session_state['search_string'] = ''

    # select which capmaign to show: Trump or Biden
    if st.session_state.view == 'dataset':
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





        default_cols=['index','file_name','create_ts','file_type']
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
                    # st.write(invoice_images)
                    byte_invoice_images = byte_images_to_show[row.file_name]                                       


                    with st.expander(f":green[Show invoice pages:] {row.file_name}"):


                        try:
                            col1, col2 = st.columns(2)
                            btn_invoice = utils.download_invoice_as_zipped_page_images(row.file_name, 
                                                                                    byte_invoice_images,
                                                                                    col1)
                            
                            btn_pdf_invoice = utils.download_invoice_as_pdf(s3_client, row.file_name, col2)

                            tab_names = [f"page {i+1}" for i in range(len(invoice_images))]
                            
                            for ind, page_tab in enumerate(st.tabs(tab_names)):
                                page_tab.image(invoice_images[ind])
                                fn=f"{row.file_name}_page_{ind}.jpg"
                                img_byte_arr = byte_invoice_images[ind]


                                btn_page = utils.download_image(fn, img_byte_arr, ind,page_tab)
                        except:
                            st.error("Failed to find images of file pages")
                            # st.error("Try to close and reopen the browser tab with the app")
                        

with tab_zero_shot:
    set_view('zero-shot')

    wrong_df = utils.load_gpt_wrong_df(s3_client, bucket = BUCKET, counter=None)

    with st.expander("Expand to see the wrong predictions by GPT-4o"):
        st.info("""The table below shows pdf invoices which GPT-4o failed to process correctly.
                Correct prediction is when line_sum (sum_total of all Spot line items) is equal to 
                the invoice gross amount). You can test GPT-4o predictions by downloading either the
                pdf file fo the invoice or images of its pages and use the together with the prompt in 
                the Chat.openai.com playgorund. You can copy our prompt below on this page or use your 
                own prompt.
                """, icon="ℹ️")

        selection_wrong_df = utils.dataframe_with_selections(wrong_df)
        if st.button(":blue[Show selected files]", key = 'btn_wrong_df'):
            if selection_wrong_df.empty:
                st.error("You have not selected any files")
                st.stop()

            else:

                wrong_df_to_show = wrong_df.loc[selection_wrong_df.index,:].copy()
                st.dataframe(wrong_df_to_show)

                pil_images_to_show, byte_images_to_show = utils.get_images_to_show(s3_client,wrong_df_to_show)

                for row in wrong_df_to_show.itertuples():
                            # st.session_state['summary_df'][row[0]] = pd.DataFrame()
                    invoice_images = pil_images_to_show[row.file_name]
                    # st.write(invoice_images)
                    byte_invoice_images = byte_images_to_show[row.file_name]                                       


                    # with st.expander(f":green[Show invoice pages:] {row.file_name}"):


                    try:
                        col1, col2 = st.columns(2)
                        btn_invoice = utils.download_invoice_as_zipped_page_images(row.file_name, 
                                                                                byte_invoice_images,
                                                                                col1)
                        
                        btn_pdf_invoice = utils.download_invoice_as_pdf(s3_client, row.file_name, col2)

                        tab_names = [f"page {i+1}" for i in range(len(invoice_images))]
                        
                        for ind, page_tab in enumerate(st.tabs(tab_names)):
                            page_tab.image(invoice_images[ind])
                            fn=f"{row.file_name}_page_{ind}.jpg"
                            img_byte_arr = byte_invoice_images[ind]


                            btn_page = utils.download_image(fn, img_byte_arr, ind,page_tab)
                    except:
                        st.error("Failed to find images of file pages")


        prompt_content = """
    ### Question1: What are the summary fields in the invoice ?

    ### Question2: How many line items are in the invoice ?

    ### Question3: How many spots are in the invoice ?

    ### Question4: What are the headers of the line items in the invoice ?

    ### Question5: Which headers correspond to:
        * TV program description
        * Amount paid for Spot
        * Spot Air Date


    ### Question6: Analyze each line item, for each header from the Answer to Question5 assign
    corresponding spot value.

    ### Question7: Add all spot_amounts and output the total sum

    ### Answer_example =
    {
    'Summary_fields' : {
                            'number': '3983920-1',
                            'date': '2023-09-29',
                            'gross_amount': 10000.00,
                            'net_amount': 9500.00,
                            'issuer': 'WTAE',
                                            }
                        },
    'Line_items_num' : 10,
    'Spots_num' : 12,
    'Headers_mapping':
                        {'description' : 'Description',
                        'spot_amount' : 'Amount',
                        'air_date' : 'Air Date'},
    'Line_items' : [{
                        'line_num': 1,
                        'spot_num' : 1,
                        'air_date' : '2024-03-13',
                        'description': '6-7am News',
                        'spot_amount': 750.00
                        },
                        {
                        'line_num': 5,
                        'spot_num' : 6,
                        'air_date' : '2024-03-15',
                        'description': '6-7am News',
                        'spot_amount': 1750.00
                        }],
    'spot_amounts_total' = 2500.00

    }


    Answer in JSON format
    """
        
        

    with st.expander("Show prompt"):
        st.code(prompt_content)


