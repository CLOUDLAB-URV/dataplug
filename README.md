# dataplug

**dataplug is a Python framework for efficiently accessing partitions of unstructured data stored in object storage for elastic workloads in the Cloud**

- Using dataplug, users can define **Cloud-native data types** for objects and files stored in Object Storage. Each type implements data-format specific preprocessing techniques to generate indexes and metadata, which are later used to partition one or more object *on-the-fly* using efficient Object Storage native APIs (HTTP PUT/GET).

- **Dataplug avoids costly ETL preprocessing jobs**, such as generating static partitions of a dataset. Static partitions are not ideal, as it imposes an arbitrary partition size that does not fit all workloads, or it may also involve potentially duplicated data.

- **Dataplug implements an extensible interface**, where users can define new cloud-native data types, new preprocessing techniques, and new partitioning strategies, so that it always fits the needs of the workload at hand. In addition, users can publish and share implemented extensions publicly via plugins for other users to use them, promoting good practices such as reusable optimized code.

- **Dataplug is serverless and is portable** for use with your favourite distributed data analytics cluster or serverless framework (Dask, Ray, PySpark, Lithops, ...) in any Cloud (AWS, Google Cloud, ...) or on-premise, without the need to manage servers for preprocessing jobs.

### Partitioning text example

```python

# Assign UTF8Text data type for object in s3://testdata/lorem_ipsum.txt
co = CloudObject.from_s3(UTF8Text, 's3://testdata/lorem_ipsum.txt')

# Generate 8 partitions, using whole_words_strategy, which avoids cutting words in half
# A data_slice is a reference to a partition, which is lazily evaluated
data_slices = co.partition(whole_words_strategy, num_chunks=8)

def word_count(data_slice):
    # Evaluate the data_slice, which will return the actual partition text
    text_chunk = data_slice.get()

    words = defaultdict(lambda: 0)
    for word in text_chunk.split():
        words[word] += 1
    return dict(words)

# Use Lithops for deploying a parallel serverless job, which will scatter generated data slices, one to each worker 
fexec = lithops.FunctionExecutor()
fexec.map(word_count, data_slices)
result = fexec.get_result()
```



