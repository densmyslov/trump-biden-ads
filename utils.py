import streamlit as st
import pandas as pd
import os
import numpy as np
import re
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import sys
import subprocess
import importlib.util
import boto3
from io import BytesIO, StringIO
import string
from random import choice, choices
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
# import login
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from PIL import Image
from zipfile import ZipFile, ZIP_DEFLATED
import json
from collections import defaultdict

load_dotenv()

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_KEY')
BUCKET = os.environ.get('BUCKET')

def create_zip(byte_invoice_images, file_name=None):
    
    zip_buffer = BytesIO()
    with ZipFile(zip_buffer, "a") as zip_file:
        for i, image_bytes in enumerate(byte_invoice_images):
            file_name = f"page_{i+1}.jpg"
            zip_file.writestr(file_name, image_bytes)
    zip_buffer.seek(0)
    return zip_buffer

def dataframe_with_selections(df: pd.DataFrame, init_value: bool = False,
                              start_date=None) -> pd.DataFrame:
    df_with_selections = df.copy()
    if start_date:
        df_with_selections = df_with_selections.query("create_ts>=@start_date")

    df_with_selections.insert(0, "Select", init_value)

    # Get dataframe row-selections from user with st.data_editor
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True),
                       'create_ts' : st.column_config.DatetimeColumn('Date',
                                                                     format='YYYY-MM-DD'),
                    'file_type': st.column_config.TextColumn('File type')
                                                                     },
        disabled=df.columns,
    )

    # Filter the dataframe using the temporary column, then drop the column
    selected_rows = edited_df[edited_df.Select]
    return selected_rows.drop('Select', axis=1)


@st.experimental_fragment
def download_image(fn, img_byte_arr, page,page_tab):
    count = choice(range(1000000))
    btn = page_tab.download_button(
                                    label="Download page",
                                    data=img_byte_arr,
                                    file_name=fn,
                                    mime="image/png",
                                    key = f"{fn}_page_{page}_{count}"
                                )
    return btn

@st.experimental_fragment
def download_invoice_as_pdf(s3_client, fn, col):
    key = f"FCC/pdfs/{fn}/doc.pdf"
    obj = s3_client.get_object(Bucket=BUCKET, Key=key)
    pdf_bytes = obj['Body'].read()
    pdf_buffer = BytesIO(pdf_bytes)
    pdf_buffer.seek(0)
    count = choice(range(1000000))
    btn = col.download_button(
                label="Download as PDF file",
                data=pdf_buffer,
                file_name=f"{fn}.pdf",
                mime="application/pdf",
                key = f"{fn}_pdf_{count}"
            )
    return btn

@st.experimental_fragment
def download_invoice_as_zipped_page_images(fn, byte_invoice_images, col):
    zip_buffer = create_zip(byte_invoice_images, fn)
    btn = col.download_button(
                label="Download All Pages as Zip",
                data=zip_buffer,
                file_name=fn,
                mime="application/zip",
                key = fn
            )
    return btn

@st.experimental_fragment
def download_invoice_as_zipped_page_images(fn, byte_invoice_images, col):
    zip_buffer = create_zip(byte_invoice_images, fn)
    btn = col.download_button(
        label="Download All Pages as Zip",
        data=zip_buffer.getvalue(),  # Ensure to pass the bytes content
        file_name=f"{fn}.zip",       # Ensure the file name has .zip extension
        mime="application/zip",
        key=fn
    )
    return btn

def get_image(s3_client,bucket, key):
    # Use the S3 client to download the file
    
    buffer= BytesIO()
    s3_client.download_fileobj(bucket, key, buffer)
    buffer.seek(0)
    pil_image = Image.open(buffer)
    # Reset buffer's pointer to the beginning
    buffer.seek(0)
    
    # Read the buffer content into bytes
    image_bytes = buffer.read()
    return pil_image, image_bytes

@st.cache_data()
def get_images_to_show(_s3_client,df_to_show):
    """
    After user selects the invoices to show, this function gets the images from S3
    """
    pil_images_to_show = {}
    byte_images_to_show = {}
    for row in df_to_show.itertuples():
        page_image_keys = [f"FCC/images/{row.file_name}/page_{page_num}.jpg" for page_num in range(20)]
        invoice_images = []
        invoice_image_bytes = []
        for page_image_key in page_image_keys:
            # st.write(page_image_key)
            try:
                page_image, page_image_bytes = get_image(_s3_client,
                                                            BUCKET, 
                                                            page_image_key)
                invoice_images.append(page_image)
                invoice_image_bytes.append(page_image_bytes)
            except:
                break


        pil_images_to_show[row.file_name] = invoice_images
        byte_images_to_show[row.file_name] = invoice_image_bytes
    return pil_images_to_show, byte_images_to_show

@st.cache_data()
def load_invoice_df(_s3_client, name, bucket = BUCKET, counter=None):
    
    key = f"datasets/FCC/{name}_df.parquet"
    # st.write(key)

    invoice_df, last_modified = pd_read_parquet(_s3_client, bucket, key)
    invoice_df.sort_values('create_ts',ascending=False, inplace=True, ignore_index=True)

    invoice_df['search_col'] = invoice_df.apply(lambda x: f"{x['file_name']} {x['file_type']}", axis=1)
    invoice_df.drop_duplicates(subset=['file_name'],ignore_index=True, inplace=True)
    return invoice_df.reset_index(), last_modified

@st.cache_data()
def load_gpt_wrong_df(_s3_client, bucket = BUCKET, counter=None):
    """
    dataset of wrong predictions on april_invoices_df from GPT-4o model
    see 'create_truth_datasets.ipynb'
    """
    key = 'datasets/FCC/completions/april_2024_biden_invoices/wrong_df.parquet'
    wrong_df, last_modified = pd_read_parquet(_s3_client, bucket, key)
    
    return wrong_df.reset_index()


def pd_read_parquet(_s3_client,bucket,key,columns=None):

    """
    Reads a Parquet file from an S3 bucket and returns a pandas DataFrame.
    """


    try:
        obj = _s3_client.get_object(Bucket=bucket,Key=key)
        last_modified = obj['LastModified'].strftime("%Y-%m-%d")
        buffer = BytesIO(obj['Body'].read())
        if columns:
            return pd.read_parquet(buffer,
                                columns=columns)
        else:
            return pd.read_parquet(buffer), last_modified
    except:
        return pd.DataFrame()


