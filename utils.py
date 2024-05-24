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
    # selected_all = st.toggle("Select all", key='select_all')
    # if selected_all:
    #     init_value = True
    df_with_selections.insert(0, "Select", init_value)

    # Get dataframe row-selections from user with st.data_editor
    edited_df = st.data_editor(
        df_with_selections,
        hide_index=True,
        column_config={"Select": st.column_config.CheckboxColumn(required=True),
                       'create_ts' : st.column_config.DatetimeColumn('Date',
                                                                     format='YYYY-MM-DD'),
                    'completion': st.column_config.TextColumn('File type')
                                                                     },
        disabled=df.columns,
    )

    # Filter the dataframe using the temporary column, then drop the column
    selected_rows = edited_df[edited_df.Select]
    return selected_rows.drop('Select', axis=1)


@st.experimental_fragment()
def download_image(fn, img_byte_arr, page,page_tab):
    btn = page_tab.download_button(
                                    label="Download page",
                                    data=img_byte_arr,
                                    file_name=fn,
                                    mime="image/png",
                                    key = f"{fn}_page_{page}"
                                )
    return btn

@st.experimental_fragment()
def download_invoice_as_image(fn, byte_invoice_images):
    zip_buffer = create_zip(byte_invoice_images, fn)
    btn = st.download_button(
                label="Download All Pages as Zip",
                data=zip_buffer,
                file_name=fn,
                mime="application/zip",
                key = fn
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
            except:
                break
            invoice_images.append(page_image)
            invoice_image_bytes.append(page_image_bytes)

        pil_images_to_show[row.file_name] = invoice_images
        byte_images_to_show[row.file_name] = invoice_image_bytes
    return pil_images_to_show, byte_images_to_show

@st.cache_data()
def load_invoice_df(_s3_client, name, bucket = BUCKET, counter=None):
    
    key = f"datasets/FCC/{name}_df.parquet"
    # st.write(key)

    invoice_df, last_modified = pd_read_parquet(_s3_client, bucket, key)
    invoice_df.sort_values('create_ts',ascending=False, inplace=True, ignore_index=True)

    MODEL = 'gemma-7b-it'
    api = 'groq'
    bucket = 'bergena-invoice-parser'
    key = f"FCC/completions/file-classification/{api}/{MODEL}/completions_df.parquet"
    completions_df, _ = pd_read_parquet(_s3_client, bucket, key)
    completions_df['completion'] = completions_df['completion'].apply(lambda x: 
                            list(json.loads(x).values())[0]
                            if isinstance(x,str) else x)
    completions_df['file_name']=completions_df['key'].str.split('/').str[2]

    invoice_df = invoice_df.merge(completions_df[['file_name',
                                                  'completion']],
                                                  on='file_name',
                                                  how = 'left')
    invoice_df['search_col'] = invoice_df.apply(lambda x: f"{x['file_name']} {x['completion']}", axis=1)
    invoice_df.drop_duplicates(subset=['file_name'],ignore_index=True, inplace=True)
    return invoice_df.reset_index(), last_modified

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


class ConstructedText(object):
  def __init__(self,
               extracted_words,
               tolerance,
               max_symbols):
    self.extracted_words = extracted_words
    self.tolerance = tolerance
    self.max_symbols = max_symbols

    self.text_lines = self.get_constructed_text_lines_from_pdf(extracted_words)
    self.constructed_text = ' '.join(self.text_lines)

  def find_group_key(self, top, groups):
    for group_key in groups.keys():
        if abs(group_key - top) <= self.tolerance:
            return group_key
    return top

  def get_constructed_text_lines_from_pdf(self,
                                          extracted_words):

      # extracted_words = extract_words_from_pdf_s3(buffer)
      sorted_words = self.sort_pdf_extracted_words(extracted_words,
                                                   round_coords = True)

      line_width = int(max([i['right'] for i in sorted_words])) + 10

      grouped_by_top = defaultdict(list)
      for ind, item in enumerate(sorted_words):

        text, top, left, right = list(item.values())
        group_key = self.find_group_key(top, grouped_by_top)
        # grouped_by_top[top].append((text, left, right))
        grouped_by_top[group_key].append((text, left,right))

      invoice_lines = []
      for item in grouped_by_top.items():
        values = item[1]
        line = self.get_line_from_item(values)
        invoice_lines.append(line)
      if self.max_symbols:
        invoice_lines = [line for line in invoice_lines if len(line.replace(' ','')) < self.max_symbols]

      return invoice_lines

  def get_line_from_item(self, items): # We start with an empty string
    line = ""

    # We initialize the end of the last word (x1) to 0
    last_x1 = 0

    # Now we loop through each item
    for text, x0, x1 in items:
        # Calculate the spaces needed before the current word starts
        space_count = x0 - last_x1

        # We add the calculated number of spaces to the line
        line += ' ' * space_count

        # We add the word itself to the line
        line += text

        # We update the last_x1 to the end of the current word
        last_x1 = x1
    return line

  def sort_pdf_extracted_words(self, extracted_words, round_coords = True):

    sorted_words = []
    page_offset = 0

    for page_num, items in extracted_words.items():


      for i in items:
          text = i['text']
          x0 = i['x0']
          x1 = i['x1']
          top = i['top']+page_offset

          if round_coords:
            x0 = int(round(x0, 0))
            x1 = int(round(x1, 0))
            top = int(round(top, 0))

          converted_coordinates = {
                            'text': text,
                            'top': top,
                            'left': x0,
                            'right': x1
                              }
          sorted_words.append(converted_coordinates)
      page_offset+=converted_coordinates['top']
    return sorted(sorted_words, key=lambda x: x['top'])


