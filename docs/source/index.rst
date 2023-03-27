Dataplug
========

`Dataplug` is a Python library that enables to **efficiently access partitions of unstructured data directly from S3 in parallel**. Distributed parallel processing frameworks (like `Dask <https://www.dask.org/>`_, `Ray <https://www.ray.io/>`_ or `Lithops <https://lithops-cloud.github.io/>`_) can leverage `dataplug` to benefit from efficient parallel partitioned data consumption for highly parallel workloads in the Cloud.

Using `dataplug`, users can define **Cloud-native data types for objects and files stored in Object Storage**. Each Cloud-native data type implements format-specific pre-processing techniques to generate indexes and metadata, which are later used to partition one or more object *on-the-fly* using efficient Object Storage native APIs (HTTP PUT/GET).

- **Dataplug avoids costly ETL preprocessing jobs, such as generating static partitions of a dataset.** Static partitions are not ideal, as it imposes an arbitrary partition size that does not fit all workloads, or it may also involve potentially duplicated data.

- **Dataplug is extensible**, new cloud-native data types, preprocessing techniques, and partitioning strategies can be defined, so that it always fits the needs of the workload at hand. In addition, users can publish and share implemented extensions publicly via plugins for other users to use them, promoting good practices such as reusable optimized code.

- **Dataplug is serverless and portable** for use with your favourite distributed data analytics cluster or serverless framework (Dask, Ray, PySpark, Lithops, ...) in any Cloud (AWS, Google Cloud, ...) or on-premise, without the need to manage servers for preprocessing jobs.


Documentation
-------------

.. toctree::
   :maxdepth: 1

   model.rst

.. toctree::
   :maxdepth: 1

   config/index.rst

.. toctree::
   :maxdepth: 1

   api_reference/index.rst

.. toctree::
   :maxdepth: 0

   types/index.rst