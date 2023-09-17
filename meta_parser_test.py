import json
#import tika
#tika.initVM()
from tika import parser

def extract_content_and_metadata(input_file):
    # Use Tika to parse the file
    parsed = parser.from_file(input_file)

    # Extract content and metadata
    content = ""
    #content = parsed.get('content')
    metadata = parsed.get('metadata')

    # Return the results
    return {'content': content, 'metadata': metadata}

def save_to_json(data, output_file):
    with open(output_file, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    input_filepath = '/Users/syedrafice/Documents/INSTALL/gcap_metadata_extract/docs/CS-25Amendment24.pdf'  # Replace with your file path
    output_filepath = 'output.json'  # Replace with desired output file path

    result = extract_content_and_metadata(input_filepath)
    save_to_json(result, output_filepath)
