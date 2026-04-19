from google.cloud import storage
from pathlib import Path


BUCKET_NAME = "mastr-pipeline-de-processed-data"

PARQUET_PATH = "/Users/saif/Desktop/mastr-pipeline/data/processed"


def uploadProcessedToGCS(bucket_name, folder_path):
    
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    path_obj = Path(folder_path)
    print(f"Starte Upload von: {folder_path}")
    for file_path in path_obj.rglob("*"):
        if file_path.is_file():
            relative_path  = file_path.relative_to(path_obj)
            blob_path = f"processed/{relative_path}"
            blob = bucket.blob(blob_path)
            blob.upload_from_filename(str(file_path))
            print(f"Uploaded: {blob_path}") 

    print("Upload Done !")


if __name__ == "__main__":
    print("Script startet!")
    uploadProcessedToGCS(bucket_name=BUCKET_NAME, folder_path=PARQUET_PATH)


    
