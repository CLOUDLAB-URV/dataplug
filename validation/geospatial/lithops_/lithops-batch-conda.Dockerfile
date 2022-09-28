FROM continuumio/miniconda3:4.10.3

RUN echo "python==3.9.13" >> /opt/conda/conda-meta/pinned

RUN apt-get --allow-releaseinfo-change update \
        && apt-get upgrade -y --no-install-recommends \
        && apt-get install -y --no-install-recommends \
           gcc \
           libc-dev \
           libxslt-dev \
           libxml2-dev \
           libffi-dev \
           libssl-dev \
           unzip \
           make \
        && rm -rf /var/lib/apt/lists/* \
        && apt-cache search linux-headers-generic

RUN conda update -n base conda \
    && conda install -c conda-forge pdal python-pdal \
    && conda clean --all

RUN pip install --upgrade --ignore-installed pip wheel six setuptools

RUN pip install --upgrade --no-cache-dir --ignore-installed \
        boto3 \
        redis \
        httplib2 \
        requests \
        numpy \
        scipy \
        pandas \
        pika \
        kafka-python \
        cloudpickle \
        ps-mem \
        tblib \
        laspy[lazrs,laszip] \
        rasterio \
        pyproj

ENV APP_HOME /lithops
WORKDIR $APP_HOME

COPY lithops_aws_batch.zip .
RUN unzip lithops_aws_batch.zip && rm lithops_aws_batch.zip

ENTRYPOINT python entry_point.py
