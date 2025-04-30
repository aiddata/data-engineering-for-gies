
import geopandas as gpd

path = "/home/userx/git/data-engineering-for-gies/data/geoBoundaries/v6_9469f09_57dcd43/geoBoundaries-GHA-ADM2/geoBoundaries-GHA-ADM2.gpkg"

gdf = gpd.read_file(path)

df = gdf[["shapeID", "shapeName"]].copy()

# random assign n units as 1, rest as 0
n = int(0.25 * len(df))
df["treatment"] = 0
df.loc[df.sample(n=n).index, "treatment"] = 1

df.to_csv("/home/userx/git/data-engineering-for-gies/data/treatment/ghana_adm2_treatment.csv", index=False)
