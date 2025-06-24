import numpy as np
from netCDF4 import Dataset,num2date
import xarray as xr
import glob
#from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from concurrent.futures import ProcessPoolExecutor

from shapely.geometry import LineString, Point

def process_year(year):
    try:
        files = sorted(glob.glob(f"{path0}{CONFIG}-VAH{VAH}_1m_grid_T_{year}*.nc"))
        if not files:
            print(f"No files found for year {year}")
            return
        print(f"Processing: {files[0]}")
       
        ds= xr.open_mfdataset(files, combine='by_coords')  # or 'nested' if needed
        T = ds["temp"]
        S = ds["salt"]
        # Extract using vectorized point indexing
        masked_temp = T.isel(y=("points", j_idx), x=("points", i_idx))
        masked_salt = S.isel(y=("points", j_idx), x=("points", i_idx))
        # Shape: (time, depth, points)
        months=ds['month']
        masked_lats = nav_lat[j_idx, i_idx]
        masked_lons = nav_lon[j_idx, i_idx]

        time_array = np.array([np.datetime64(f"{year}-{month:02d}") for month in months])
    ###########now open the ptrc
        # --- Load PTRC (biogeochemical) data ---
        ptrc_files = sorted(glob.glob(f"{path0}{CONFIG}-VAH{VAH}_1m_ptrc_T_{year}*.nc"))
        if not ptrc_files:
            print(f"No PTRC files found for year {year}")
            return

        ds_ptrc = xr.open_mfdataset(ptrc_files, combine='by_coords')
        O2 = ds_ptrc["O2"].isel(y=("points", j_idx), x=("points", i_idx))
        Alk = ds_ptrc["Alkalini"].isel(y=("points", j_idx), x=("points", i_idx))
        DIC = ds_ptrc["DIC"].isel(y=("points", j_idx), x=("points", i_idx))
        NO3 = ds_ptrc["NO3"].isel(y=("points", j_idx), x=("points", i_idx))

    #####################%%%%%%%%%%%%%%%%%%%
        
        ds_out = xr.Dataset(
            {
                "temp": (("time", "depth", "point"), masked_temp.data),
                "salt": (("time", "depth", "point"), masked_salt.data),
                "O2": (("time", "depth", "point"), O2.data),
                "TAlk": (("time", "depth", "point"), Alk.data),
                "DIC": (("time", "depth", "point"), DIC.data),
                "NO3": (("time", "depth", "point"), NO3.data),
            },
            coords={
                "time": ("time", time_array),
                "depth": ("depth", depth),
                "point": np.arange(len(masked_lats)),
                "lat": ("point", masked_lats),
                "lon": ("point", masked_lons),
            }
        )


        # Add metadata 
        ds_out["temp"].attrs["units"] = "degC"
        ds_out["salt"].attrs["units"] = ""
        ds_out["depth"].attrs["units"] = "m"
        ds_out["lat"].attrs["units"] = "degrees_north"
        ds_out["lon"].attrs["units"] = "degrees_east"
        ds_out["time"].attrs["standard_name"] = "time"
        ds_out["O2"].attrs["units"] = "mmol/m3"
        ds_out["TAlk"].attrs["units"] = "mmol/m3"
        ds_out["DIC"].attrs["units"] = "mmol/m3"
        ds_out["NO3"].attrs["units"] = "mmol/m3"
        path_out='/gpfs/fs7/dfo/hpcmc/pfm/amh001/DATA/hackathon/'
        # Save to NetCDF
        outfile = f"{path_out}lineP_band_subset_y{year}_1deg.nc"
        print(outfile)
#        encoding = {
 #           var: {"zlib": True, "complevel": 4} for var in ds_out.data_vars
  #      }

   #     ds_out.to_netcdf(outfile, format="NETCDF4", encoding=encoding)
        #ds_out.to_netcdf(outfile, format="NETCDF4", encoding={"temperature": {"zlib": True, "complevel": 4}})
        ds_out.to_netcdf(outfile, format="NETCDF4")

    except Exception as e:
        print(f"[ERROR] Failed to process year {year}: {e}")
        traceback.print_exc()
#########################################
namdepth='/home/amh001/space_fs7/DATA/NEP36-I/HINDCAST/inputs_714x1020/mesh_mask_NEP36_GLORYS12v1OBC_10.nc'
with Dataset(namdepth, 'r') as f:
    mask0 = f['tmask'][0,0,:,:].filled().astype(bool)
    j, i = np.where(mask0)
    ny, nx = mask0.shape
    nav_lat = f['nav_lat'][:,:]
    nav_lon = f['nav_lon'][:,:]
    depth = f['gdept_1d'][0,:]
ly,lx=nav_lat.shape

# Flattened lat/lon
lat_flat = nav_lat.ravel()
lon_flat = nav_lon.ravel()
#load the csv with lineP locations

csv_path = '/gpfs/fs7/dfo/hpcmc/pfm/amh001/TOOLS/python/hackathon/LineP.csv'
df = pd.read_csv(csv_path)
lats = df['latitude'].values
lons = df['longitude'].values
line_coords = list(zip(lons, lats))  # length 26
track = LineString(line_coords)
track_buffer = track.buffer(1) # degree buffer alongt he line

# Mask points inside buffer
mask_flat = np.array([
    track_buffer.contains(Point(lon, lat))
    for lon, lat in zip(lon_flat, lat_flat)
])
j_idx, i_idx = np.unravel_index(np.where(mask_flat)[0], nav_lat.shape)
 
VAH='1000'
CONFIG='NEP36-CanOE-TKE'
path0='/home/amh001/work_fs7/RUN_DIR/Auto-restart/NEP36-CanOE-TKE/HINDCAST/'
print(path0)
# Use ThreadPoolExecutor to parallelize year processing

years = range(1996, 2025)
with ProcessPoolExecutor(max_workers=32) as executor:
    executor.map(process_year, years)
#with ThreadPoolExecutor(max_workers=32) as executor:  # Adjust `max_workers` based on your system
#    executor.map(process_year, years)

