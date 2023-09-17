from tika import parser
from elasticsearch import Elasticsearch

# Read configurations from .config file
configurations = {}
with open('.config', 'r') as f:
    for line in f.readlines():
        key, value = line.strip().split('^')

        configurations[key] = value

def extract_data_from_file(file_path):
    """Extract content and metadata from a file using Apache Tika."""
    parsed = parser.from_file(file_path)
    return {
        'content': parsed.get('content'),
        'metadata': parsed.get('metadata', {})
    }

def index_into_elasticsearch(cloud_id, es_host, index_name, es_user, es_password, data):
    """Index data and metadata into Elasticsearch."""
    try:
        es = Elasticsearch(
        """ Commented out Cloud ID as this is only used for Elastic ESS """
        # cloud_id=cloud_id,
        host=es_host
        basic_auth=(es_user, es_password)
    )
    except Exception as e:
        print ("Error connecting to Elastic")
        raise e
    else:
        print ("Connected to Elastic")
    
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)
    
    res = es.index(index=index_name, document=data)
    return res

if __name__ == "__main__":
    try:
        extracted_data = extract_data_from_file(configurations["FILE_PATH"])
    except Exception as e:
        print ("Error extracting content/metadata from file with Tika")
        raise e
    else:
        print ("Extraction from Tika Successful")
    
    if extracted_data:
        result = index_into_elasticsearch(
            configurations["CLOUD_ID"], 
            configurations["ES_HOST"], 
            configurations["INDEX_NAME"], 
            configurations["ES_USER"], 
            configurations["ES_PASSWORD"], 
            extracted_data
        )
        print(f"Data indexed with ID: {result['_id']}")
    else:
        print("No data extracted from the file.")
