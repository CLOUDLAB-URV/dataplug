Cloud-native data model
=======================

This page introduces the core concepts of the **Cloud-native data types** model and how `dataplug` implements it.

Concepts
--------

Cloud-native data types
.......................

In the **Cloud-native data types** model model, each object in Cloud object has an associated **Cloud-native data type.**

.. raw:: html

   <hr>

.. rst-class:: centered lead

A **Cloud-native data type** is a conceptual extension of an existing unstructured scientific data format, which enables
to access its data content in parallel when the data is stored in object storage, with the objective to apply efficient
partitioning of these data for distributed computing workloads in the Cloud.

.. raw:: html

   <hr>

It is a *conceptual extension* because the structure of the data formats is not altered. Instead, we only
require **read-only pre-processing to generate metadata, indexes, and extract attributes**, which are
stored *outside* of the data file. This metadata is later used to obtain granular portions of the data,
taking into account native object stores APIs (byte-range HTTP GET requests). The only catch is that metadata must be
reusable and to enable parallel data access for any given granularity, so that data pre-processing has
to be performed only once.

A **Cloud-native data type** is composed of the following elements:

- An arbitrary data blob :math:`\delta` that complies to a well-known format.
- An arbitrary data structure :math:`\delta_{m}` which describes and indexes the content of :math:`\delta` (i.e. metadata from :math:`\delta`).
- A set of key-value attributes :math:`\mathcal{A}` related to :math:`\delta` and :math:`\delta_{m}`.
- A pre-processing function :math:`PreProc(\delta) = (\delta_{m}, \mathcal{A})`, that when applied on :math:`\delta` outputs metadata :math:`\delta_{m}` and attributes :math:`\mathcal{A}`.
- A set of partitioning strategies :math:`\mathcal{P}`, so that :math:`p(\delta, \delta_{m}) =\\ \{s_{0}, s_{1}, \ldots, s_{n}\}`, being $p$ a concrete partitioning strategy from :math:`\mathcal{P}` and :math:`s` a *data slice*.

To promote an existing unstructured data format to be a **Cloud-native data type**, the model requires implementing all elements described above specifically for that particular format.
A **Cloud-native object** is a particular object (e.g. ``s3://my-bucket/imporant-data.csv``) which has an associated **Cloud-native data type** (``Cloud-native CSV``).

There are already some Cloud-native data types implemented for varous domains, which are documented here.

**Data pre-processing:** Before enabling parallel data access to a Cloud-native data type, this has to be first
pre-processed. Each type has a defined pre-processing method, which extracts characteristics from the raw data blob
content, such as metadata, internal structure, indices, and/or attributes specific to the data format. `Dataplug` is extensible,
and does not impose any restriction on the metadata structure or on its generation process, so that new types can be implemented
and adapted to each workload as needed.

Extracted metadata is stored *outside* of the input data blob. This implies that read-only data access is required
for use in `dataplug`, for example, for data stored in the `AWS Registry of Open Data <https://registry.opendata.aws/>`_.
Nevertheless, `dataplug` provides unified access to data and metadata, so the end user does not have to manually
manage metadata and data separately.

Partitioning strategy
.....................

After applying pre-processing and extracting metadata, applications are enabled to access in parallel to **Cloud-native type's** data contents.
For it, a **partitioning strategy** has to be performed first.

.. raw:: html

   <hr>

.. rst-class:: centered lead

A **partitioning strategy** is a function associated to a **Cloud-native data type**, which based on the metadata and indexes
generated at the pre-processing stage and some user-defined arguments, it calculates a set of **data slices** which
represent a virtual partitioned view of a Cloud-native object.

.. raw:: html

   <hr>

To generate partitions, the **partitioning strategy** function works on the metadata and
indexes generated at the pre-processing stage and not directly with the actual data.
The result of applying a **partitioning strategy** is a set of **data slices**.

Data slice
..........

.. raw:: html

   <hr>

.. rst-class:: centered lead

A **data slice** is an entity that represents a lazily-evaluated partitioned view of a Cloud-native data type.
It encapsulates metadata and code. When evaluated, the slice code fetches and applies the necessary corrections
to recover the actual data partition content.


.. raw:: html

   <hr>

Evaluating a **data slice** to fetch the data partition content could imply performing one or more byte-range HTTP GET
requests against object storage to obtain specific chunks of one or more objects,
which can be assembled in the data analysis process local memory if necessary.
The procedure and metadata needed to retrieve and assemble the partition contents are embedded within each **data slice**.

Users can leverage different strategies with different parameters that generate many **data slices**, which can be
adapted to each particular workload case, avoiding restrictions such as pre-set partition size.

**Data partitioning:** To partition a **Cloud-native object**, we first need to apply partitioning strategy on a
specific object instance to generate a set of *data slices*. Each *data slice* is then scattered many
distributed workers, which will evaluate the *data slice* code to load the contents of the data partition
into memory. This process is done in parallel, exploiting the high synchronization-free parallel access and high
bandwidth of object stores. *Data slices* must be serializable to be sent to remote worker processes.

Contrary to static partitioning, which requires processing all the dataset and creating fixed-sized partitions,
our partitioning method allows the generation of dynamic partitions on-the-fly to adapt to different workloads,
enabling for more efficient workload balancing.


Architecture
------------

`Dataplug`'s primary goal is to simplify the whole management life cycle of unstructured scientific data in the Cloud.
More specifically:

- Facilitate the management of unstructured data pre-processing and its metadata. `Dataplug` provides mechanisms to pre-process data in parallel with automatic resource provisioning depending on the data volume to be pre-processed.

- Facilitate the partitioning of unstructured data to enable parallel access to it directly from object storage for Cloud-based distributed scientific workloads. `Dataplug` is compatible with popular distributed computing frameworks such as PySpark, Dask, or Ray, thus achieving portability and standardization for many possible workloads.

`Dataplug` places special emphasis on being extensible to all levels, as to allow the implementation of new Cloud-native
data model features (such as new types, pre-processing techniques and partitioning strategies, as well as pre-processing
execution backends).

.. figure:: images/framework-architecture.png
   :align: center
   :width: 650
   :alt: Dataplug Architecture

   Dataplug Architecture


1. First, **raw data** is stored in a bucket in object storage as objects. This data has not been pre-processed yet. The framework does not require write permissions to the bucket data source, so data could also be sourced from a read-only public Open Data Repository bucket.

2. Raw data can be promoted to be *Cloud-native* by applying the corresponding **pre-processing** to generate metadata for the raw's data type. The pre-processing stage extracts attributes and generates metadata from the raw data which are stored in a metadata bucket in object storage.

3. After applying *pre-processing* to the raw data, `Dataplug` allows to query *Cloud-native objects* and access to the metadata and attributes and finally **apply a partitioning strategy** to create *data slices* for a certain workload.

4. The user can now **submit a parallel processing job** using some Python distributed computing framework (Ray, Dask, Lithops...) and pass the *data slice* set created as input data for the job. The distributed computing framework will take care of deploying distributed workers and scatter the *data slices* among them.

5. A worker receives as input a *data slice*, which can be evaluated to **fetch the data partition content**. Using embedded metadata and code, the *data slice* can perform byte-range HTTP GET requests over one or more objects from object storage to retrieve data chunks, assemble them and perform the necessary corrections before finally passing it back to the worker job processes logic.


Example application: FASTQGZip partitioning
-------------------------------------------

Here it is described a full example of `dataplug`'s functionality for the ``FASTQGZip`` Cloud-native data type.

Pre-processing
..............

With `Dataplug`, users can define and implement *Cloud-Native data types* as Python classes decorated with
``CloudDataType``, where we define the associated pre-processor and the type hierarchy.

.. figure:: images/fastqgz-step1.png
   :align: center
   :alt: Workflow step 1: Create a new Cloud-native data type

   Workflow step 1: Create a new Cloud-native data type

.. code-block:: python

    @CloudDataType(preprocessor=GZipPreprocessor)
    class FASTQGZip:
      number_of_sequences: int
      experiment_id: str
      ...  # additional attributes


In the code above we can see an example where we define a new *Cloud-native data type* for ``FASTQGZip`` data. We can
define multiple attributes, in this case, the number of genome sequences and experiment id.

.. code-block:: python

    class GZipPreprocessor(BatchPreprocessor):
      @staticmethod
      def preprocess(self, cloud_object: CloudObject,
          experiment_id: str):
        stream = cloud_object.s3.get_object()
        # process 'stream' to generate index using gztool
        n_lines, index = generate_gzip_index(stream)
        n_sequences = no_lines // 4
        return PreprocessingMetadata(
          attributes={'number_of_sequences': no_sequences,
                      'experiment_id': experiment_id},
          metadata=index)


Following the example, in the code above we see the implementation of a *batch* type pre-processor for the ``FASTQGZip``
*Cloud-native data type*. `gztool <https://github.com/circulosmeos/gztool>`_ is used to generate a GZip index.

We can now reference an object stored in S3 using its full URI location (``s3://my_bucket/SRR123456.fastqgz``), assign
it the ``FASTQGZip`` *Cloud-native data type*, and apply the corresponding pre-processing using the
``AWSEC2Preprocesor`` backend.

.. code-block:: python

    # Create Cloud Object reference
    co = CloudObject.from_s3(FASTQGZip,
                             's3://my_bucket/SRR123456.fastqgz')

.. figure:: images/fastqgz-step2.png
   :align: center
   :alt: Workflow step 2: Assign a Cloud-native data type to a object stored in S3

   Workflow step 2: Assign a Cloud-native data type to a object stored in S3

.. code-block:: python

    # Preprocess object (this has to be done only once)
    backend = AWSEC2Preprocessor()
    co.preprocess(backend, sequence_identifier='SRR0000000')



.. figure:: images/fastqgz-step3.png
   :align: center
   :alt: Workflow step 3: Apply partitioning strategy and generate data slices

   Workflow step 3: Apply partitioning strategy and generate data slices



Partitioning
............

Next, we need to implement a *partitioning strategy* for the ``FASTQGZip`` Cloud-native data type.

.. code-block:: python

    def partition_num_reads(cloud_object: FASTQGZip,
        num_seq_partition: int) -> List[FASTQGZipTextSlice]:
      # Split by number of reads per worker
      # (each read is composed of 4 lines)
      n_lines = cloud_object["number_of_sequences"] * 4
      n_parts = (num_seq_partition * 4) // n_lines
      lpp = ceil(n_lines / n_parts) # lines per partition
      linepairs = [((lpp * i) + 1, (lpp * i) + lpp + 1)
                   for i in range(n_parts)]
      byteranges = get_byteranges(cloud_object.metadata, linepairs)
      return [FASTQGZipTextSlice(range_0, range_1)
              for (range_0, range_1) in byteranges]

The code above implements partition strategy that calculates the offsets of the GZip file entry points for each
partition, based on the number of lines per partition. Other strategies can be implemented, for example, to partition
by total number of chunks, regardless of the chunk size. Pre-processing strategy functions can access attributes and
metadata generated in the pre-processing phase. In this case, we utilize the GZip index generated earlier to calculate the necessary offsets.

The result of this function is a list of *GZipTextSlice*, which embeds in each *data slice* the necessary metadata
to later retrieve the data chunk:

.. code-block:: python

    class GZipTextSlice(CloudObjectSlice):
      def get(self):
        index = self.cloud_object.metadata
        byterange = f'bytes={self.range_0}-{range_1}'
        stream = self.s3.get_object(Range=byterange)
        lines = (self.line_0, self.line_1)
        chunk = decompress_gzip(stream, index, lines=lines)
        return chunk




Each worker of the distributed computing framework will receive an instance of the ``GZipTextSlice`` classe, one for
each slice generated in the partitioning strategy. The `get()` method will be called by the application user code,
which evaluates the *data slice* to perform a byte-range GET request to object storage with the specified range,
and, using the index, it finally returns the specified decompressed chunk of the ``FASTQGZip`` file.

We can now apply the partitioning strategy and create *data slices*:

.. code-block:: python

    # Apply partition strategy and get slices
    data_slices = co.partition(partition_num_reads,
                               num_seq_partition=1_000_000)


.. figure:: images/fastqgz-step4.png
   :align: center
   :alt: Workflow step 4: Apply partitioning strategy and generate data slices

   Workflow step 4: Apply partitioning strategy and generate data slices


.. code-block:: python

    # Define processing function
    def process_sequences(data_slice: GZipTextSlice):
      chunk = data_slice.get()
      ...  # process fastq chunk
      return result

    # Submit distributed parallel job with ipyparallel
    # with data slices as input data
    with ipyparallel.Cluster() as cluster:
      view = cluster.load_balanced_view()
      result = view.map(process_sequences, data_slices)

.. figure:: images/fastqgz-step5.png
   :align: center
   :alt: Workflow step 5: Scatter data slices and consume partitions in parallel from many distributed workers

   Workflow step 5: Scatter data slices and consume partitions in parallel from many distributed workers



