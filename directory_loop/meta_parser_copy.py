import os
from tika import parser
from elasticsearch import Elasticsearch, helpers, exceptions

# Read configurations from .config file
configurations = {}
with open('.config_copy', 'r') as f:
    for line in f.readlines():
        key, value = line.strip().split('^')
        configurations[key] = value

def extract_data_from_file(file_path):
    try:
        """Extract content and metadata from a file using Apache Tika."""
        parsed = parser.from_file(file_path)
        return {
            'content': parsed.get('content'),
            'metadata': parsed.get('metadata', {})
        }
    except Exception as e:
        print(f"Error extracting data from {file_path}. Reason: {e}")
        return None

def prepare_for_bulk(data, index_name):
    """Prepare data for the Elasticsearch bulk API."""
    for item in data:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_source": item
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
        exit(1)  # Exit the script

    try:
        if not es.indices.exists(index=configurations["INDEX_NAME"]):
            es.indices.create(index=configurations["INDEX_NAME"])
    except exceptions.RequestError as re:
        print(f"Error creating index. Reason: {re}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    data_list = []
    directory_path = configurations["DIRECTORY_PATH"]
    
    try:
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)

            if os.path.isfile(file_path):
                extracted_data = extract_data_from_file(file_path)
                print(f"Extracted {filename} from Tika")
                if extracted_data:
                    data_list.append(extracted_data)
                else:
                    print(f"No data extracted from {filename}.")
    except Exception as e:
        print(f"Error processing files in directory {directory_path}. Reason: {e}")

    if data_list:
        try:
            # Using the bulk API to index the data
            helpers.bulk(es, prepare_for_bulk(data_list, configurations["INDEX_NAME"]))
            print("All data indexed successfully.")
        except exceptions.BulkIndexError as bie:
            print(f"Bulk indexing error: {bie}")
        except Exception as e:
            print(f"Unexpected error during indexing: {e}")
    else:
        print("No data to index.")
