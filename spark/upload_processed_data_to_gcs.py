from google.cloud import storage
from pathlib import Path
import os


# GCP Credentials für Kestra
creds_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
if creds_json:
    creds_path = "/tmp/gcp_creds.json"
    with open(creds_path, "w") as f:
        f.write(creds_json)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path

BUCKET_NAME = "mastr-pipeline-de-processed-data"

PARQUET_PATH = os.environ.get("PROCESSED_DATA_PATH", "/project/data/processed")


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


    
