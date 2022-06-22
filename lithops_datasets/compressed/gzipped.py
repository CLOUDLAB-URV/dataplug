import threading

from ..cloudobjectbase import CloudObjectBase, partitioner_strategy

import subprocess


class GZippedBlob(CloudObjectBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self):
        streaming_body = self.storage.get_object(bucket=self.bucket, key=self.key, stream=True)
        pipe = subprocess.Popen(['/home/lab144/.local/bin/gztool', '-i', '-x', '-s' '10'], stdin=subprocess.PIPE,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        def _write_feeder():
            chunk = streaming_body.read(65536)
            while chunk != b"":
                pipe.stdin.write(chunk)
                chunk = streaming_body.read(65536)

        write_feeder = threading.Thread(target=_write_feeder)
        write_feeder.start()
        write_feeder.join()
        stdout, stderr = pipe.communicate()
        print(stdout, stderr)


class GZippedText(GZippedBlob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @partitioner_strategy
    def even_lines(self):
        pass
