import inspect

import joblib

from .handler import monolith_joblib_handler, map_joblib_handler, reduce_joblib_handler


# Process the entire object as one batch job
def monolithic_preprocessing(cloud_object, parallel_config, preprocessing_function, extra_args):
    preproc_signature = inspect.signature(preprocessing_function).parameters

    if "cloud_object" not in preproc_signature:
        raise Exception("Preprocessing function must have cloud_object as a parameter")

    preproc_args = {"cloud_object": cloud_object}

    # Add extra args if there are any other arguments in the signature
    for arg in preproc_signature.keys():
        if arg not in preproc_args and arg in extra_args:
            preproc_args[arg] = extra_args[arg]

    with joblib.parallel_config(**parallel_config):
        jl = joblib.Parallel()
        gen = jl([joblib.delayed(monolith_joblib_handler)((preprocessing_function, preproc_args))])
        # joblib returns a generator
        res = list(gen).pop()


# Partition the object in chunks and preprocess it in parallel
def mapreduce_preprocessing(cloud_object, parallel_config, chunk_size, preprocessing_function, finalizer_function,
                            extra_args):
    preproc_signature = inspect.signature(preprocessing_function).parameters
    if not {"chunk_data", "chunk_id", "chunk_size", "num_chunks"}.issubset(preproc_signature.keys()):
        raise Exception("Preprocessing function must have "
                        "(chunk_data, chunk_id, chunk_size, num_chunks) as parameters")

    jobs = []
    num_chunks = cloud_object.size // chunk_size
    for chunk_id in range(num_chunks):
        preproc_args = {"cloud_object": cloud_object, "chunk_id": chunk_id, "chunk_size": chunk_size,
                        "num_chunks": num_chunks, "chunk_data": None}
        # Add extra args if there are any other arguments in the signature
        for arg in preproc_signature.keys():
            if arg not in preproc_args:
                preproc_args[arg] = extra_args[arg]
        jobs.append(preproc_args)

    with joblib.parallel_config(**parallel_config):
        jl = joblib.Parallel()
        # Run partial chunking preprocessing jobs in parallel
        gen = jl([joblib.delayed(map_joblib_handler)((preprocessing_function, job)) for job in jobs])
        # joblib returns a generator
        partial_results = list(gen)

        # Sort partial results by chunk_id
        partial_results.sort(key=lambda x: x[0])

        # Run finalizer function to merge all partial results
        args = {"cloud_object": cloud_object, "partial_results": partial_results}
        gen = jl([joblib.delayed(reduce_joblib_handler)((finalizer_function, args))])
        res = list(gen).pop()
