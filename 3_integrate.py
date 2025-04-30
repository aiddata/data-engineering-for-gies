"""
"""
from pathlib import Path
from functools import reduce

import pandas as pd
import geopandas as gpd
import rasterstats as rs

from config import get_config


config = get_config()

# ---------------------------------------


base_path = Path(config["base_path"])

treatment_path = base_path / config["treatment_path"]

adm2_path = base_path / "geoBoundaries" / f'{config["boundary"]["version"]}_{config["boundary"]["gb_data_hash"]}_{config["boundary"]["gb_web_hash"]}' / "geoBoundaries-GHA-ADM2" / "geoBoundaries-GHA-ADM2.geojson"

adm1_path = base_path / "geoBoundaries" / f'{config["boundary"]["version"]}_{config["boundary"]["gb_data_hash"]}_{config["boundary"]["gb_web_hash"]}' /"geoBoundaries-GHA-ADM1" / "geoBoundaries-GHA-ADM1.geojson"


raster_items = {f"esa_lc_{year}": base_path / "esa_landcover" / f"esa_lc_{year}.tif" for year in config["landcover"]["years"]}


output_csv_path = base_path / "output" / "ghana_adm2_data.csv"
output_geojson_path = base_path / "output" / "ghana_adm2_data.geojson"


# -------------------------------------




adm2_gdf = gpd.GeoDataFrame.from_file(adm2_path)
adm1_gdf = gpd.GeoDataFrame.from_file(adm1_path)

adm2_gdf = adm2_gdf.drop(columns=["shapeISO", "shapeGroup", "shapeType"])
adm1_gdf = adm1_gdf.drop(columns=["shapeISO", "shapeGroup", "shapeType"])

adm2_gdf = adm2_gdf.sjoin(adm1_gdf, how="inner", predicate="intersects", lsuffix="adm2", rsuffix="adm1")

adm2_gdf = adm2_gdf.merge(adm1_gdf[["shapeID", "geometry"]], how="left", left_on="shapeID_adm1", right_on="shapeID", suffixes=("", "_adm1"))

adm2_gdf = adm2_gdf.drop(columns=["index_adm1", "shapeID"])


adm2_gdf["overlap_adm1"] = adm2_gdf.apply(lambda x: x.geometry.intersection(x.geometry_adm1).area / x.geometry.area, axis=1)

# keep only instance of shapeID_adm2 with the largest overlap_adm1
adm2_gdf = adm2_gdf.loc[adm2_gdf.groupby("shapeID_adm2")["overlap_adm1"].idxmax()]


assert adm2_gdf.overlap_adm1.min() > 0.5, "Overlap is less than 50% for some polygons"


# ---------------------------------------


treatment_df = pd.read_csv(treatment_path)

adm2_gdf = adm2_gdf.merge(treatment_df[["shapeID", "treatment"]], how="left", left_on="shapeID_adm2", right_on="shapeID")
adm2_gdf = adm2_gdf.drop(columns=["shapeID"])


# ---------------------------------------


id_field = "shapeID_adm2"

category_map = config["landcover"]["category_map"]

category_map = {v: k for k, v in category_map.items()}

results = [adm2_gdf]

for raster_id, raster_path in raster_items.items():
    tmp_stats = rs.zonal_stats(
        adm2_gdf,
        raster_path,
        geojson_out=True,
        prefix=f"{raster_id}_",
        categorical=True,
        category_map=category_map,
        all_touched=True)
    tmp_gdf = gpd.GeoDataFrame.from_features(tmp_stats)
    tmp_gdf = tmp_gdf.fillna(0)
    tmp_cols = tmp_gdf.columns
    tmp_gdf = tmp_gdf[[i for i in tmp_gdf.columns if i not in results[0].columns or i == id_field]]
    results.append(tmp_gdf)


# merge results into single gdf
output_gdf = reduce(lambda left,right: pd.merge(left, right, on=id_field, how='inner'), results)

output_gdf = output_gdf.drop(columns=["geometry_adm1"])


# ---------------------------------------


output_csv_path.parent.mkdir(parents=True, exist_ok=True)

# write csv
output_gdf[[i for i in output_gdf.columns if i != "geometry"]].to_csv(output_csv_path, index=False, encoding='utf-8')

# write geojson
output_gdf.to_file(output_geojson_path, driver="GeoJSON", encoding='utf-8')
