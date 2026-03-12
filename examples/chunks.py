"""
Example of using a generator to process geospatial features

Note: The memory profile may be inaccurate if you run both full and chunked sections at the same time.
For most accurate results, comment out one section at a time and run them each separately.
"""
import time
import geopandas as gpd
from memory_profiler import profile

# download from https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM1.gpkg
input_path = "/home/userx/Desktop/geoBoundariesCGAZ_ADM1.gpkg"

# -------------------------------------

@profile
def calc_area_full(input_path):
    """
    Read a file and return the geodataframe
    """
    gdf = gpd.read_file(input_path)
    area_full = gdf['geometry'].area.sum()

    return area_full

full_start_time = time.time()

area_full = calc_area_full(input_path)

full_end_time = time.time()

print(f"Total area (without chunks): {area_full}")

print(f"Time taken to read without chunks: {full_end_time - full_start_time:.2f} seconds")

# -------------------------------------

@profile
def read_file_in_chunks(input_path, chunk_size):
    """
    Generator function to read a file in chunks
    """
    # track chunk index between loop iterations
    i = 0
    while True:
        # print(f"Reading chunk {i}...")

        # calculate slice beginning and end for this chunk
        chunk = slice(i, i + chunk_size)

        # read chunk from input file
        gdf_iter = gpd.read_file(input_path, rows=chunk)

        if len(gdf_iter) == 0:
            # print(f"\tFinished reading all chunks.")
            break

        yield gdf_iter

        # update chunk index
        i += chunk_size


@profile
def calc_area_chunks(input_path):
    # set size of each chunk (as large as possible)
    chunk_size = 1000

    area_list = []
    for gdf_iter in read_file_in_chunks(input_path, chunk_size):
        # run processing on chunk gdf
        area_list.append(gdf_iter['geometry'].area.sum())


    area_chunked = sum(area_list)
    return area_chunked


chunk_start_time = time.time()

area_chunked = calc_area_chunks(input_path)

chunk_end_time = time.time()


print(f"Total area (with chunks): {area_chunked}")

print(f"Time taken to read with chunks: {chunk_end_time - chunk_start_time:.2f} seconds")
