Architecture
============

`Dataplug`'s primary goal is to simplify the whole management life cycle of unstructured scientific data in the Cloud.
More specifically:

- Facilitate the management of unstructured data pre-processing and its metadata. `Dataplug` provides mechanisms to pre-process data in parallel with automatic resource provisioning depending on the data volume to be pre-processed.

- Facilitate the partitioning of unstructured data to enable parallel access to it directly from object storage for Cloud-based distributed scientific workloads. `Dataplug` is compatible with popular distributed computing frameworks such as PySpark, Dask, or Ray, thus achieving portability and standardization for many possible workloads.

`Dataplug` places special emphasis on being extensible to all levels, as to allow the implementation of new Cloud-native
data model features (such as new types, pre-processing techniques and partitioning strategies, as well as pre-processing
execution backends).

.. figure:: images/framework-architecture.png
   :align: center
   :width: 700
   :alt: Dataplug Architecture

   Dataplug Architecture


1. First, **raw data** is stored in a bucket in object storage as objects. This data has not been pre-processed yet. The framework does not require write permissions to the bucket data source, so data could also be sourced from a read-only public Open Data Repository bucket.

2. Raw data can be promoted to be *Cloud-native* by applying the corresponding **pre-processing** to generate metadata for the raw's data type. The pre-processing stage extracts attributes and generates metadata from the raw data which are stored in a metadata bucket in object storage.

3. After applying *pre-processing* to the raw data, `Dataplug` allows to query *Cloud-native objects* and access to the metadata and attributes and finally **apply a partitioning strategy** to create *data slices* for a certain workload.

4. The user can now **submit a parallel processing job** using some Python distributed computing framework (Ray, Dask, Lithops...) and pass the *data slice* set created as input data for the job. The distributed computing framework will take care of deploying distributed workers and scatter the *data slices* among them.

5. A worker receives as input a *data slice*, which can be evaluated to **fetch the data partition content**. Using embedded metadata and code, the *data slice* can perform byte-range HTTP GET requests over one or more objects from object storage to retrieve data chunks, assemble them and perform the necessary corrections before finally passing it back to the worker job processes logic.

