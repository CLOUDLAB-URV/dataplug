FROM ghcr.io/dask/dask:2022.9.1-py3.9

RUN conda update -n base conda \
    && conda install -c conda-forge pdal python-pdal \
    && conda clean --all \

RUN pip install --upgrade --no-cache-dir --ignore-installed \
        boto3 \
        laspy[lazrs,laszip] \
        rasterio \
        pyproj \