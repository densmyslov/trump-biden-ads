# Trump and Biden for President Campaigns
### collection of PDF documents, such as contracts, orders, invoices and other documents
This repository is a demo of processing a collection of historical PDF documents of various types with a view to save manual hours of potentially highly skilled personnel.  

The main processing tasks:
* pre-processing (page image and text retrieval) and storage in AWS s3 buckets
* visualization with [streamlit app](https://trump-biden.streamlit.app/)
* using LLMs to classify PDFs by document type (since we don't know which class each pdf document belongs to)
* retrieval of values 
* using LLMs to retrieve values from PDF documents
* creating vector embeddings and index to implement semantic search and RAG
