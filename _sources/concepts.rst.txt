Concepts
========

.. vale off

This page introduces the core concepts of the **Cloud-native data types** model.

Cloud-native data types
-----------------------

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

Data partitioning
-----------------

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