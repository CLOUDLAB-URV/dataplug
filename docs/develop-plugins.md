# Developing a new plugin

To develop a new plugin to support a new data format, you need to implement the following interface:

#### 1. A `CloudDataFormat` class decorator.
This class decorator will define the data format's name,the preprocessing function to be applied to the raw data, and all the attributes that the data format will have.

```python
@CloudDataFormat(preprocessing_function=preprocess_my_new_format)
class NewPlugin:
    attribute1: str
    attribute2: int
    ...
```

#### 2. A preprocessing function (e.g. `preprocess_my_new_format`).
This function will receive the raw data as input and will return the preprocessed data.

```python
def preprocess_my_new_format(cloud_object: CloudObject) -> PreprocessingMetadata:
    # Here you can access to the object's body using cloud_object
    
    with cloud_object.open("r") as f:
        ...
      
    body = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)['Body'] 
    
    # You must return a PreprocessingMetadata object
    return PreprocessingMetadata(
        attributes={
            # Here you must mut the key-value attributed pairs that were defined in the CloudDataFormat class
        },
        metadata=...,  # Here you can return any binary metadata that you want to store. You must take care of serialization.
        metadata_file_path=... # Or you can return a file path to the metadata file
    )
```

The previous function will process data in a batch, meaning that the whole object will be preprocessed at once.


Data can also be pre-processed in parallel by chunking the data blob. The pre-processing function will be applied to
each of the chunks, and then, a finalizer function will be called once to merge the results, in a map-reduce style.

First, you must define the preprocessing and finalizer functions in the `CloudDataFormat` decorator.
```python
@CloudDataFormat(preprocessing_function=preprocess_my_new_format, finalizer_function=finalizer_for_my_new_format)
```

Then, the preprocessing function will need the following signature:

```python
def preprocess_my_new_format(cloud_object: CloudObject, chunk_data: StreamingBody,
                             chunk_id: int, chunk_size: int, num_chunks: int):
    ...
```

Chunk data is a `StreamingBody` object that contains the chunk data. The chunk_id is the chunk number, starting from 0. The chunk_size is the size of the chunk in bytes, and num_chunks is the total number of chunks.

The finalizer function will have the following signature:

```python
def merge_fasta_metadata(cloud_object: CloudObject, chunk_metadata: List[PreprocessingMetadata]):
    # Here you need to merge the metadata from all the chunks
    # And finally return a single PreprocessingMetadata object
    return PreprocessingMetadata(...)
```

`chunk_metadata` is a list of `PreprocessingMetadata` objects, containing the result of each pre-processed chunk.

#### 3. Slices

A slice is a reference to a partition, which is lazily evaluated. Slices are created by calling the `partition` method on a `CloudObject` instance.
Slices must extend from `CloudObjectSlice` and implement the `get` method, which will return the actual partition data.

```python
class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()
        
        
class MyNewFormatSlice(CloudObjectSlice):
    def get(self):
        # Here you can consult your metadata generated for this format
        metadata = self.cloud_object.s3.get_object(
                Bucket=self.cloud_object.meta_path.bucket,
                Key=self.cloud_object.meta_path.bucket
        )["Body"]
        
        # Perform the necessary HTTP GET operations to get the data
        chunk = self.cloud_object.s3.get_object(
                Bucket=self.cloud_object.path.bucket,
                Key=self.cloud_object.path.bucket,
                Range=f"bytes={self.range_0}-{self.range_1}"
        )["Body"]
        
        # And finally return the actual chunked data        
        return chunk
```

#### 4. Partitioning strategies

Finally, you can define partitioning strategies for your new plugin. A partitioning strategy is a function that read the metadata and define a list of slices.
For partitioning strategies, you must use the `PartitioningStrategy` class decorator and specify the data format that the strategy will be used for.


```python
@PartitioningStrategy(dataformat=NewPlugin)
def newformat_partitioning_strategy(cloud_object: CloudObject, num_chunks: int):
    slices = []
    for i in range(num_chunks):
        # Here you put the necessary logic for defining the byte ranges required to read a chunk of the data
        range_0 = ...
        range_1 = ...
        slice = MyNewFormatSlice(range_0, range_1)
        slices.append(slice)
    return slices
```

#### 5. Using the new plugin

Once you have implemented the plugin, you can use it as follows:

```python
import logging

from dataplug import CloudObject

# Here you import your new plugin, which will be located in another module
from mynewplugin import NewPlugin, newformat_partitioning_strategy

if __name__ == "__main__":
    # Point to your data in S3, using your format defined in the new plugin
    co = CloudObject.from_s3(NewPlugin, "s3://path/to/your/data")

    # Preprocess the data, this only needs to run once
    co.preprocess(parallel_config={})

    # Now you can partition the data using the partitioning strategy
    data_slices = co.partition(newformat_partitioning_strategy, num_chunks=8)
    
    # Now you can scatter the data slices to remote workers
    # Here we simply iterate the list and get the data
    for data_slice in data_slices:
        data = data_slice.get()
        print(data)
```
