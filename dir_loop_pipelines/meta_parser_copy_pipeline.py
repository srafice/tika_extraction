import os
from tika import parser
from elasticsearch import Elasticsearch, helpers, exceptions

# Read configurations from .config file
configurations = {}
with open('.config_copy_pipeline', 'r') as f:
    for line in f.readlines():
        key, value = line.strip().split('^')  # Use '^' as delimiter
        configurations[key] = value

def extract_data_from_file(file_path):
    """Extract content and metadata from a file using Apache Tika."""
    try:
        parsed = parser.from_file(file_path)
        return {
            'content': parsed.get('content'),
            'metadata': parsed.get('metadata', {})
        }
    except Exception as e:
        print(f"Error extracting data from {file_path}. Reason: {e}")
        return None

def ensure_index_exists(es_instance, index_name):
    """Ensure that the given index exists. If not, create it."""
    if not es_instance.indices.exists(index=index_name):
        es_instance.indices.create(index=index_name)

def prepare_for_bulk_content(data, index_name, pipeline_name):
    """Prepare content data for the Elasticsearch bulk API."""
    for item in data:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_source": {"content": item["content"]},
            "pipeline": pipeline_name
        }

def prepare_for_bulk_metadata(data, index_name, pipeline_name):
    """Prepare metadata for the Elasticsearch bulk API."""
    for item in data:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_source": item["metadata"],
            "pipeline": pipeline_name
        }

if __name__ == "__main__":
    try:
        es = Elasticsearch(
            """ Commented out Cloud ID as this is only used for Elastic ESS """
            # cloud_id=configurations["CLOUD_ID"],
            host=configurations["ES_HOST"],
            basic_auth=(configurations["ES_USER"], configurations["ES_PASSWORD"])
        )
    except Exception as e:
        print(f"Error connecting to Elasticsearch. Reason: {e}")
        exit(1)

    ensure_index_exists(es, configurations["CONTENT_INDEX_NAME"])
    ensure_index_exists(es, configurations["METADATA_INDEX_NAME"])

    data_list = []
    directory_path = configurations["DIRECTORY_PATH"]
    
    try:
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            if os.path.isfile(file_path):
                extracted_data = extract_data_from_file(file_path)
                if extracted_data:
                    print(f"Successfully extracted data from {filename}.")
                    data_list.append(extracted_data)
                else:
                    print(f"No data extracted from {filename}.")
    except Exception as e:
        print(f"Error processing files in directory {directory_path}. Reason: {e}")

    content_data_list = [{"content": item["content"]} for item in data_list]
    metadata_data_list = [{"metadata": item["metadata"]} for item in data_list]

    # Index content
    if content_data_list:
        try:
            print("Indexing extracted content data...")
            helpers.bulk(
                es, 
                prepare_for_bulk_content(content_data_list, configurations["CONTENT_INDEX_NAME"], configurations["CONTENT_INGEST_PIPELINE"])
            )
            print("All content data indexed successfully.")
        except exceptions.BulkIndexError as bie:
            print(f"Bulk indexing error for content: {bie}")
        except Exception as e:
            print(f"Unexpected error during content indexing: {e}")

    # Index metadata
    if metadata_data_list:
        try:
            print("Indexing extracted metadata data...")
            helpers.bulk(
                es, 
                prepare_for_bulk_metadata(metadata_data_list, configurations["METADATA_INDEX_NAME"], configurations["METADATA_INGEST_PIPELINE"])
            )
            print("All metadata indexed successfully.")
        except exceptions.BulkIndexError as bie:
            print(f"Bulk indexing error for metadata: {bie}")
        except Exception as e:
            print(f"Unexpected error during metadata indexing: {e}")
