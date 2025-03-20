import os
import sys
import json
import duckdb as duckdb

def read_path_from_mapping_file(mapping_file_path: str, model_name: str) -> str:
    """
    Read the ParquetPath for the given model_name from the mapping file.
    Args:
        mapping_file_path (str): The path to the mapping file.
        model_name (str): The name of the model to search for.
    Returns:
        str: The ParquetPath for the given model_name, or None if not found.
    """
    with open(mapping_file_path, 'r') as file:
        data = json.load(file)
        for item in data:
            if item.get('Name') == model_name:
                return item.get('ParquetPath')

def find_subdirectories(root_dir: str) -> list[str]:
    """
    Find all subdirectories of the given root directory.
    Args:
        root_dir (str): The root directory to search in.
    Returns:
        list[str]: A list of all subdirectories.
    """
    subdirectories = []
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for dirname in dirnames:
            subdirectories.append(os.path.join(dirpath, dirname))
    return subdirectories

def configure_duck_views(model_name: str, verbose_log: bool = False) -> None:
    """
    Configure DUCK views for the given model_name.
    Args:
        model_name (str): The name of the model to configure views for.
        verbose_log (bool, optional): Whether to print verbose log messages. Defaults to False.
    """
    try:
        duckFilePath: str = os.environ.get('duck_db_path', "/output/solution_views.ddb")
        print(f'Setting up DUCK Views - using {duckFilePath}')
        
        mapping_file_path = os.environ.get('directory_map_path', "/simulation/directorymapping.json")
        model_directory = read_path_from_mapping_file(mapping_file_path, model_name)
        
        if model_directory is None:
            raise Exception(f"Unable to find output for model name provided {model_name}")
        else:
            print(f"Solution data found for {model_name}: {model_directory}")
        directories = find_subdirectories(model_directory)
        
        with duckdb.connect(duckFilePath) as con:
            for item in directories:
                view_name = item.replace(model_directory, '').replace('/', '').replace('\\', '')
                path = os.path.join(item, "**", "*.parquet")
                if "datadataFileId=" in view_name:
                    break
                view_command = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM '{path}';"
                if verbose_log:
                    print(view_command)
                    con.execute(view_command)
                    con.sql(f"select * from {view_name} limit 2;").show()
                else:
                    con.execute(view_command)
    except Exception as e:
        print('Configuring DUCK Views failed due to an exception:')
        print(e)

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            model_name = sys.argv[1]
            verbose_log = len(sys.argv) > 2 and sys.argv[2].lower() in ['1', 'true']
            configure_duck_views(model_name, True)
        else:
            print("Model Name parameter is required.")
    except ValueError as e: 
        print('Configuring duck views failed:')
        print(e)
    finally:
        print("done")