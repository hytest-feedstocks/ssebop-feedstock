from datetime import date

import apache_beam as beam
import pandas as pd
import xarray as xr

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import Indexed, ConsolidateMetadata, OpenURLWithFSSpec, OpenWithXarray, StoreToZarr, T

input_url_pattern = (
    'https://edcintl.cr.usgs.gov/downloads/sciweb1/shared/uswem/web/'
    'conus/eta/modis_eta/daily/downloads/'
    'det{yyyyjjj}.modisSSEBopETactual.zip'
)

start = date(2000, 1, 1)
end = date(2022, 10, 7)
dates = pd.date_range(start, end, freq='1D')


def make_url(time: pd.Timestamp) -> str:
    return input_url_pattern.format(yyyyjjj=time.strftime('%Y%j'))


#pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1), file_type='tiff') # test if this is confused by .zip
pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1))


class Preprocess(beam.PTransform):
    """Preprocessor transform."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        import io
        from fsspec.implementations.zip import ZipFileSystem
        # postprocessing
        import rioxarray
        import numpy as np
        
        index, f = item

        zf = ZipFileSystem(f)
        zip_tiff = zf.open(zf.glob('*.tif')[0])
        
        tiff_bytes_io = io.BytesIO(zip_tiff.read())

        #return index, tiff_bytes_io
        # post processing
 
        time_dim = index.find_concat_dim('time')
        time_index = index[time_dim].value
        time = dates[time_index]
        ds = rioxarray.open_rasterio(tiff_bytes_io, band_as_variable=True)
        ds = ds.rename({'x': 'lon', 'y': 'lat', 'band_1': 'aet'})
        ds = ds.assign_coords(time=time).expand_dims(dim="time")

        # Save monthly compiled dataset
        #ds['aet'] = ds['aet'].where(ds['aet'] != 9999)
        ds['aet'].attrs = {}
        ds['aet'] = ds['aet'].assign_attrs(
             _FillValue = 9999,
             #scale_factor = 0.001, # not working with recipes, might need to set at a later step
             units = 'mm',
             long_name = 'SSEBOP Actual ET (ETa)',
             standard_name = 'ETa',
         )

        ds['lon'] = ds['lon'].assign_attrs(
             units = 'degree_east',
             standard_name = 'longitude',
             description = 'Longitude of the center of the grid cell',
             axis ='Y',
        )

        ds['lat'] = ds['lat'].assign_attrs(
             units = 'degree_north',
             standard_name = 'latitude',
             description = 'Latitude of the center of the grid cell',
             axis ='X',
        )

        ds['time'] = ds['time'].assign_attrs(
             standard_name = 'date',
             axis ='T',
        )

        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)

recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | Preprocess()
    | StoreToZarr(
        store_name='ssebop-us.zarr',
        combine_dims=pattern.combine_dim_keys,
        #target_chunks={'time': int(8316 / 84), 'lat': int(2834 / 26), 'lon': int(6612 / 58)},
        target_chunks={'time': 1, 'lat': int(2834 / 26), 'lon': int(6612 / 58)},
    )
    | ConsolidateMetadata()
)
