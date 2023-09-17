# tika_extraction

A python project to extract content and metadata from binary documents and ingests them into Elasticsearch

## Sample Code Folders

### Things to note

 - The files use .config files to pass settings to the script. The script processes the options by splitting the Name and Values with the "^" separator

### There are several folders which contain different use cases:

* **single_file**

This script processes a single file and loads the extracted Content and Metadata into an Elasticsearch index.

* **directory_loop**

This script processes all files in a specficied directory and loads the extracted Content and Metadata into an Elasticsearch index.

* **dir_loop_pipelines**

This script processes all files in a specficied directory and loads the extracted Content and Metadata into seperate Elasticsearch indexes respectivly and sends them via an Ingest Pipeline.