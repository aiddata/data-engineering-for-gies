"""
Example of using a generator to process geospatial features
"""
import fiona
import geopandas as gpd
from memory_profiler import profile

# download from https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/CGAZ/geoBoundariesCGAZ_ADM1.gpkg
input_path = "/home/userx/Desktop/geoBoundariesCGAZ_ADM1.gpkg"

@profile
def gen_features(input_path):
    with fiona.open(input_path) as src:
        for feature in src:
            yield feature

@profile
def test_gen():
    area = []
    for feat in gen_features(input_path):
        tmp_gdf = gpd.GeoDataFrame.from_features([feat], )
        area.append(tmp_gdf.geometry[0].area)
        # perform feature processing here
    return area

gen_area = test_gen()

@profile
def test_all():
    gdf = gpd.read_file(input_path)
    return gdf.area

all_area = test_all()
