Quickstart (Local Docker deployment)
====================================

Here we provide a quickstart guide to test your ``dataplug`` installation is working.

First, we need an S3 compatible object storage, e.g. MinIO. You can deploy a MinIO using ``docker``:

.. code-block:: bash

    docker run -p 9000:9000 -p 9001:9001 -v ~/minio-data:/data quay.io/minio/minio server /data --console-address ":9001"

Next, set up the default keys for MinIO:

.. code-block:: bash

    export AWS_ACCESS_KEY_ID="minioadmin"
    export AWS_SECRET_ACCESS_KEY="minioadmin"

We need some test data in our local MinIO deployment. `Install <https://aws.amazon.com/cli/>`_ the ``aws-cli`` so we can create a bucket and upload a test object.

.. code-block:: bash

    aws configure set default.s3.signature_version s3v4

Create a test bucket:

.. code-block:: bash

    aws --endpoint-url http://127.0.0.1:9000 s3api create-bucket --bucket test-bucket

And upload some test text file:

.. code-block:: bash

   wget -qO - https://www.gutenberg.org/cache/epub/1513/pg1513.txt | s3 cp - s3://test-bucket/moby-dick.txt

We can now reference this object from ``dataplug`` API and apply some partitioning to the text file:

.. code-block:: python

    from dataplug import CloudObject
    from dataplug.basic.text import UTF8Text, whole_words_strategy

    co = CloudObject.from_s3(UTF8Text, 's3://test-bucket/moby-dick.txt',
                             s3config={'endpoint_url': 'http://127.0.0.1:9000'})
    data_slices = co.partition(whole_words_strategy, num_chunks=200)

    for data_slice in data_slices[5:]:
        print(data_slice.get())
        print('-----')










