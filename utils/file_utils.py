import os, shutil, glob, zipfile

def clear_data(path):

    print("Clearing old files...")
    for filename in os.listdir(path):
        file_path = os.path.join(path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))
    
def unzip_files(zip_dir, extract_dir):

    os.makedirs(extract_dir, exist_ok=True)

    zip_paths = sorted(glob.glob(f"{zip_dir}/*.zip"))

    if not glob.glob(os.path.join(extract_dir, "*")):
        for zip_path in zip_paths:
            with zipfile.ZipFile(zip_path, "r") as zf:
                zf.extractall(extract_dir)

    data_files = (
        glob.glob(os.path.join(extract_dir, "*.CSV")) +
        glob.glob(os.path.join(extract_dir, "*.csv")) +
        glob.glob(os.path.join(extract_dir, "*.txt"))
    )
    clear_data(zip_dir)

    return data_files
