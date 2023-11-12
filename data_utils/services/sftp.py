import pysftp
import logging
import os


DEFAULT_DOWNLOAD_FOLDER = os.environ.get("DEFAULT_DOWNLOAD_FOLDER",".tmp")

class SFTP(pysftp.Connection):
    
    def __init__(self, host, username, password, default_path=None, **other_kwargs):
        self._sftp_live = False
        self._transport = None

        if not other_kwargs.get("cnopts"):
            self._cnopts = pysftp.CnOpts() 
            self._cnopts.hostkeys = None

        logging.info(f'Creating sftp connection to {host}' )
        self.host = host
        self.username = username
        self.password = password
        self.other_kwargs = other_kwargs
        self.default_path = default_path

    
    def __enter__(self, *args):
        super().__init__(self.host, self.username, password=self.password, 
                         cnopts=self._cnopts, default_path=self.default_path, **self.other_kwargs)
        return self
    

    def __exit__(self, *args):   
        pass 


    def list_files(self, path=".", extention=None, order_last_modified=True):

        if order_last_modified:
            all_files = self.listdir(path)
        else:
            [x.filename for x in sorted(self.listdir_attr(path), key = lambda f: f.st_mtime)]
        filtered_files = []

        if extention:
            filtered_files = [f for f in all_files if extention in f]

        files = filtered_files or all_files
        logging.info(f'Found {len(files)} sftp files', )
        return filtered_files or all_files
    

    def download_file(self, file_name, local_path=None):
        current_sftp_folder = self.getcwd()
        sftp_file_path = f"{current_sftp_folder}/{file_name}"

        if not local_path:
            local_path = f"{DEFAULT_DOWNLOAD_FOLDER}/{file_name}"

        logging.info(f"Downloading sftp file {sftp_file_path} to {local_path}")
        self.get(sftp_file_path, local_path, preserve_mtime=True)

        return local_path
