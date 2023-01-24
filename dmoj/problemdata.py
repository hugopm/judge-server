import os, tempfile, zipfile, logging
import boto3, botocore
from dmoj.config import ConfigNode, InvalidInitException

log = logging.getLogger(__name__)

session = boto3.session.Session()
client = session.client('s3',
                        region_name=os.getenv('AWS_REGION'),
                        aws_access_key_id=os.getenv('AWS_KEY'),
                        aws_secret_access_key=os.getenv('AWS_SECRET'))
                        

class ProblemLocation:
    def __init__(self, bucket, root_dir):
        self.bucket = bucket
        self._rd = root_dir

    @property
    def root_dir(self) -> str:
        # Add trailing /
        return os.path.join(self._rd, '')

    def __str__(self) -> str:
        return "s3://" + os.path.join(self.bucket, self.root_dir)

class ProblemDataManager(dict):
    problem_loc : ProblemLocation
    def __init__(self, problem_loc, **kwargs):
        super().__init__(**kwargs)
        self.problem_loc = problem_loc
        self.archive = None
        self.arch_temp_file = None
        # Cache object list
        self.obj_list = map(lambda x: x['Key'], client.list_objects(
            Bucket = problem_loc.bucket,
            Prefix = problem_loc.root_dir
        )['Contents'])

    def __missing__(self, key):
        object_name = os.path.join(self.problem_loc.root_dir, key)
        if object_name in self.obj_list: 
            with tempfile.TemporaryFile() as fp:
                client.download_fileobj(
                    self.problem_loc.bucket,
                    object_name,
                    fp)
                fp.seek(0)
                return fp.read().decode()
        elif self.archive:
            zipinfo = self.archive.getinfo(key)
            with self.archive.open(zipinfo) as f:
                return f.read()
        raise KeyError('file "%s" could not be found in "%s"' % (key, self.problem_loc))

    def __del__(self):
        if self.archive:
            self.archive.close()
            self.arch_temp_file.close()

    def resolve_archive(self, config):
        if config.archive:
            archive_path = os.path.join(self.problem_loc.root_dir, config.archive)
            try:
                self.arch_temp_file = tempfile.TemporaryFile()
                client.download_fileobj(
                    self.problem_loc.bucket,
                    archive_path,
                    self.arch_temp_file)
                self.archive = zipfile.ZipFile(self.arch_temp_file, 'r')           
            except botocore.exceptions.ClientError as e:
                self.arch_temp_file.close()
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    raise InvalidInitException('archive file "%s" does not exist' % archive_path)
                else:
                    raise e
            except zipfile.BadZipfile:
                self.arch_temp_file.close()
                raise InvalidInitException('bad archive: "%s"' % archive_path)
