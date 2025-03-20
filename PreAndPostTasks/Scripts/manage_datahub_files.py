import os
import sys
from eecloud.cloudsdk import CloudSDK
from eecloud.models import *

def download(file_pattern: str) -> None:
    try:
        cli_path = os.environ.get("cloud_cli_path")
        pxc = CloudSDK(cli_path=cli_path)

        output_path = os.environ.get('output_path', "/output")  

        print(f"Downloading Data: {file_pattern} to: {output_path} using SDK")

        download_response = pxc.datahub.download(remote_glob_patterns=[file_pattern], output_directory=output_path, print_message=True)
    except Exception as e: 
        print('Downloading Parquet Data using SDK Failed:')
        print(e)
    finally:
        print("done")
        
def upload(file_pattern: str, remote_path:str) -> None:
    try:
        cli_path = os.environ.get("cloud_cli_path")
        pxc = CloudSDK(cli_path=cli_path)

        output_path = os.environ.get('output_path', "/output")
        
        print(f"Uploading Data: {file_pattern} to: {remote_path} using SDK")
        upload_response = pxc.datahub.upload(local_folder=output_path, remote_folder=remote_path, glob_patterns=[file_pattern], is_versioned=False, print_message=True)
        
    except Exception as e: 
        print('Uploading Parquet Data using SDK Failed:')
        print(e)
    finally:
        print("done")

def main():
    if len(sys.argv) < 3:
        raise Exception("datahub pattern parameter is required.")
    
    if sys.argv[1] == 'download':
        download(sys.argv[2])
        
    elif sys.argv[1] == 'upload':
        if len(sys.argv) < 2:
            raise Exception("datahub pattern parameter is required.") 
        upload(sys.argv[2], sys.argv[3])
        
    else:
        raise Exception("upload or download with parameters are expected") 

if __name__ == "__main__":
    main()