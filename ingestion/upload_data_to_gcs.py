from google.cloud import storage
from pathlib import Path


BUCKET_NAME = "mastr-pipeline-de-raw-data"
DATA_SOUREC_LOC = "data"


def UploadDataToGCS(bucket_name, source_folder ):

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    local_path = Path(source_folder)
    files = list(local_path.glob("*.json"))

    for file in files:

        filter_id = file.name.split('_')[0]

        destination_blob_name = f"raw/{filter_id}/{file.name}"

        blob = bucket.blob(destination_blob_name)

        try:
            blob.upload_from_filename(str(file))
        except Exception as e:
            print (f"Error uploading {file.name } : {e} ")

if __name__ == "__main__":

    UploadDataToGCS(bucket_name=BUCKET_NAME, source_folder=DATA_SOUREC_LOC)