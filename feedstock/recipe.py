# FAILS

from datetime import date

import apache_beam as beam
import pandas as pd
import xarray as xr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import Indexed, OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, T

input_url_pattern = (
    #'zip+'
    'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/uswem/web/'
    'conus/eta/modis_eta/daily/downloads/'
    'det{yyyyjjj}.modisSSEBopETactual.zip'
    #'!/det{yyyyjjj}.modisSSEBopETactual.tif'
)


start = date(2000, 1, 1)
end = date(2022, 10, 7)
dates = pd.date_range(start, end, freq='1D')


def make_url(time: pd.Timestamp) -> str:
    return input_url_pattern.format(yyyyjjj=time.strftime('%Y%j'))


#pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1), file_type='tiff') # test if this is confused by .zip
pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1))

class UnzipFSSpec(beam.PTransform):
    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        from fsspec.compression import unzip
        index, f = item
        print(index)
        filename = '{index}.tiff'.format(index=1)
        #import pdb; pdb.set_trace()
        buf = unzip(f).read()

        with open(filename, 'wb') as o:
            o.write(buf)
                
        return index, filename

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)


class Preprocess(beam.PTransform):
    """Preprocessor transform."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[xr.Dataset]:
        import numpy as np
        index, ds = item

        time_dim = index.find_concat_dim('time')
        time_index = index[time_dim].value
        time = dates[time_index]

        ds = ds.rename({'x': 'lon', 'y': 'lat', 'band_data': 'aet'})
        ds = ds.drop('band')
        
        ds['aet'] = ds['aet'].where(ds['aet'] != 9999)
        #ds['aet'].assign_attrs(
        #    scale_factor = 1/1000,
        #    units = 'mm',
        #    long_name = 'SSEBOP Actual ET (ETa)',
        #    standard_name = 'ETa',
        #)
        ds = ds.expand_dims(time=np.array([time]))

        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)

#| OpenWithXarray(file_type=pattern.file_type, xarray_open_kwargs={'engine': 'rasterio'})
recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec() #open_kwargs={'compression': 'zip'})
    | UnzipFSSpec()
    | OpenWithXarray(xarray_open_kwargs={'engine': 'rasterio'})
    | Preprocess()
    | StoreToZarr(
        store_name='us-ssebop.zarr',
        combine_dims=pattern.combine_dim_keys,
        #target_chunks={'time': int(8316/84), 'lat': int(2834 / 26), 'lon': int(6612 / 58)},
        target_chunks={'time': 1, 'lat': int(2834 / 26), 'lon': int(6612 / 58)},
    )
)
# | beam.Map(test_ds) # can we assign attributes after StoreToZarr
