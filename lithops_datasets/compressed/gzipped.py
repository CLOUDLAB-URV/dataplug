import asyncio
import threading

from ..cloudobjectbase import CloudObjectBase, partitioner_strategy

import subprocess


class GZippedBlob(CloudObjectBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def preprocess(self):
        with self.cloud_object.s3.open(self.cloud_object.full_obj_key, 'rb') as co:

            proc = await asyncio.create_subprocess_exec('/home/lab144/.local/bin/gztool', '-i', '-x', '-s', '10',
                                                        stdin=asyncio.subprocess.PIPE,
                                                        stdout=asyncio.subprocess.PIPE,
                                                        stderr=asyncio.subprocess.PIPE)

            async def input_writer():
                input_chunk = await co.read(65536)
                while input_chunk != b"":
                    proc.stdin.write(input_chunk)
                    input_chunk = await self.input_stream.read(65536)
                logger.debug('done writing input')

            async def output_reader():
                output_chunk = await proc.stdout.read()
                while output_chunk != b"":
                    await self.response.send(output_chunk)
                    output_chunk = await proc.stdout.read()
                logger.debug('done reading output')

            await asyncio.gather(input_writer(), output_reader())

            pipe = subprocess.Popen(['/home/lab144/.local/bin/gztool', ],
                                    stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
        stdout, stderr = pipe.communicate()
        print(stdout, stderr)


class GZippedText(GZippedBlob):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @partitioner_strategy
    def even_lines(self):
        pass
