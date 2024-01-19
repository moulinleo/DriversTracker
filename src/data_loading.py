import pandas as pd
import dask.bag as _DaskBag
import dask.dataframe as _DaskDataFrame
import json
from src.utils import timing_decorator


@timing_decorator
def load_data_json(file_path, lib='dask'):
    if lib == 'dask':
        # Read with Dask
        df = _DaskBag.read_text(file_path).map(json.loads)
        
    elif lib == 'pandas':
         df = pd.read_json(file_path)
    else:
        raise ValueError("Unsupported library. Choose either 'dask' or 'pandas'")
    return df 

@timing_decorator
def load_data_csv(file_path, lib='dask'):
    if lib == 'dask':
        # Read with Dask
        df = _DaskDataFrame.read_csv(file_path)
    elif lib == 'pandas':
        # Read with pandas
        df = pd.read_csv(file_path)
    return df 

