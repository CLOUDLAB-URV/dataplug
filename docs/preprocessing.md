# Preprocessing

Dataplug leverages [joblib](https://joblib.readthedocs.io/en/latest/) to deploy a preprocessing jobs.
Joblib allows to use distributed backends to parallelize and scale the preprocessing tasks.

Dataplug allows to pass a configuration to joblib to use a distributed backend, for instance, to use dask distributed.

```python
co = CloudObject.from_s3(CSV, "s3://dataplug/some_csv_data.csv")

parallel_config = {"verbose": 10}  # Here you put the joblib configuration, for instance, use backend="dask" to use dask distributed
co.preprocess(parallel_config=parallel_config)
```

The `parallel_config` parameter is directly passed to joblib when a Parallel instance is created.
You can read more in the [joblib documentation](https://joblib.readthedocs.io/en/latest/generated/joblib.parallel_config.html).

