import tarfile

# Open the tar file for reading
tar_file_name = "soccer_0123.tar"
with tarfile.open(f"tar_files/{tar_file_name}", "r") as tar:
    # Extract the contents of the tar file to the current directory (BASIC folder)
    tar.extractall()
