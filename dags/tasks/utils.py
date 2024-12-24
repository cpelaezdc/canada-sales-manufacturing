from datetime import datetime
import os

def find_oldest_file(folder_path):
  """
  Finds the oldest file in a given folder based on its creation time.

  Args:
    folder_path: The path to the folder containing the files.

  Returns:
    The full path of the oldest file, or None if no files are found.
  """
  try:
    files = os.listdir(folder_path)
    oldest_file = None
    oldest_time = None

    for file in files:
      file_path = os.path.join(folder_path, file)
      creation_time = datetime.fromtimestamp(os.path.getctime(file_path)) 

      if oldest_time is None or creation_time < oldest_time:
        oldest_file = file_path
        oldest_time = creation_time

    return oldest_file

  except FileNotFoundError:
    print(f"Folder not found: {folder_path}")
    return None
  except Exception as e:
    print(f"An error occurred: {e}")
    return None

def clean_directory(prior_task,ti):
    """Finds all files in a directory that start with 'chunk'.
         Args:
            directory: The directory to search.
        Returns:
            A list of file paths.
    """
    try:
        file_to_delete = ti.xcom_pull(key='file_name', task_ids=prior_task)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(f"File to delete: {file_to_delete}")
    os.remove(file_to_delete)
    print(f"File {file_to_delete} deleted")
