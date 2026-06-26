import json

def read_metadata(metadata_file):
    with open(metadata_file,'r') as file:
        return json.load(file)

def write_metadata(metadata_file,metadata):
    with open(metadata_file,'w') as file:
        json.dump(metadata,file,indent=4)

def get_last_processed_id(metadata_file):
    metadata = read_metadata(metadata_file)
    return metadata['last_processed_id']

def update_last_processed_id(metadata_file,latest_id):
    metadata = read_metadata(metadata_file)
    metadata['last_processed_id'] = latest_id
    write_metadata(metadata_file,metadata)