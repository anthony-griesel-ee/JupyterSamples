import os

def remove_unnecessary_files(files_to_remove: list[str])-> None:
    for file in files_to_remove:
        print(f"Cleaning {file}")
        os.remove(file) 

if __name__ == "__main__":
    output_path = os.environ.get('output_path', "/output") 
    memberships_file_path = os.path.join(output_path, "memberships_data.csv")
    files_to_remove = [memberships_file_path]
    remove_unnecessary_files(files_to_remove)
    print("done")
