"""
Script to download and process geoBoundaries data

https://www.geoboundaries.org/api.html
"""
from pathlib import Path
from typing import List, Optional
import json
import logging

import shapely
import requests
import geopandas as gpd

from config import get_config


config = get_config()


def get_api_url(url: str):
    """
    Get the API URL and return the JSON object of content
    """
    response = requests.get(url)
    content = response.json()
    return content


class geoBoundariesDataset():

    name = "geoBoundaries"

    def __init__(self,
                version: str,
                gb_data_hash: str,
                gb_web_hash: str,
                output_dir: str,
                overwrite_existing: bool,
                dl_iso3_list: Optional[List[str]] = None):


        self.output_dir = output_dir / f"{version}_{gb_data_hash}_{gb_web_hash}"

        self.overwrite_existing = overwrite_existing

        # leave blank / set to None to download all ISO3 boundaries
        self.dl_iso3_list = dl_iso3_list

        self.api_url = f"https://raw.githubusercontent.com/wmgeolab/gbWeb/{gb_web_hash}/api/current/gbOpen/ALL/ALL/index.json"

        self.default_meta = {
            "name": None,
            "path": None,
            "file_extension": ".geojson",
            "title": None,
            "description": None,
            "tags": ["geoboundaries", "administrative", "boundary"],
            "citation": "Runfola, D. et al. (2020) geoBoundaries: A global database of political administrative boundaries. PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866",
            "source_name": "geoBoundaries",
            "source_url": "geoboundaries.org",
            "other": {},
            "group_name": None,
            "group_title": None,
            "group_class": None,
            "group_level": None,
        }


    def get_logger(self):
        """
        Retrieve and return the base logger to be used for this dataset
        """
        return logging.getLogger("boundary")


    def prepare(self):
        """
        Prepare data for download

        Retrieves the list of boundaries to download from the geoBoundaries API
        and filters it based on the provided ISO3 list.
        """
        logger = self.get_logger()

        logger.info(f"Preparing list of boundaries to download")

        api_data = get_api_url(self.api_url)

        if self.dl_iso3_list:
            ingest_items = [(i,) for i in api_data if i["boundaryISO"] in self.dl_iso3_list]
        else:
            ingest_items = [(i,) for i in api_data]

        ingest_items = sorted(ingest_items, key=lambda d: d[0]['boundaryISO'])
        return ingest_items


    def dl_gb_item(self, item: dict):
        """
        Download and process a single geoBoundaries item

        Prepared the metadata and downloads the boundary data from geoBoundaries.
        """
        logger = self.get_logger()

        iso3 = item["boundaryISO"]

        adm_meta = self.default_meta.copy()

        adm_meta["name"] = f"gB_v6_{iso3}_{item['boundaryType']}"

        logger.info(f"Processing geoBoundaries item: {adm_meta['name']}")

        adm_meta[
            "title"
        ] = f"geoBoundaries v6 - {item['boundaryName']} {item['boundaryType']}"
        adm_meta[
            "description"
        ] = f"This feature collection represents the {item['boundaryType']} level boundaries for {item['boundaryName']} ({iso3}) from geoBoundaries v6."
        adm_meta["group_name"] = f"gb_v6_{iso3}"
        adm_meta["group_title"] = f"gB v6 - {iso3}"
        adm_meta["group_class"] = (
            "parent" if item["boundaryType"] == "ADM0" else "child"
        )
        adm_meta["group_level"] = int(item["boundaryType"][3:])

        # save full metadata from geoboundaries api to the "other" field
        adm_meta["other"] = item.copy()

        # Example URL:
        # "https://github.com/wmgeolab/geoBoundaries/raw/c0ed7b8/releaseData/gbOpen/AFG/ADM0/geoBoundaries-AFG-ADM0.geojson",
        commit_dl_url = item["gjDownloadURL"]

        fname = Path(commit_dl_url).stem
        dir_path = self.output_dir / fname
        dir_path.mkdir(exist_ok=True, parents=True)

        gpkg_path = dir_path / f"{fname}.gpkg"
        adm_meta["path"] = str(gpkg_path)

        geojson_path = gpkg_path.with_suffix(".geojson")
        json_path = gpkg_path.with_suffix(".meta.json")

        if gpkg_path.exists() and geojson_path.exists() and json_path.exists() and not self.overwrite_existing:
            logger.info(f"Skipping existing file: {gpkg_path}")
            return

        logger.debug(f"Downloading {commit_dl_url} boundary")
        try:
            gdf = gpd.read_file(commit_dl_url)
        except:
            if requests.get(commit_dl_url).status_code == 404:
                logger.error(f"404: {commit_dl_url}")
                return
            else:
                try:
                    raw_json = get_api_url(commit_dl_url)
                    gdf = gpd.GeoDataFrame.from_features(raw_json["features"])
                except:
                    logger.error(f"Failed to download {commit_dl_url}")
                    return


        if "shapeName" not in gdf.columns:
            potential_name_field = f'{item["boundaryType"]}_NAME'
            if potential_name_field in gdf.columns:
                gdf["shapeName"] = gdf[potential_name_field]
            else:
                gdf["shapeName"] = None

        # gdf.to_file(gpkg_path, driver="GPKG")
        gdf.to_file(geojson_path, driver="GeoJSON")

        logger.debug(f"Getting bounding box for {commit_dl_url}")
        spatial_extent = shapely.box(*gdf.total_bounds).wkt
        adm_meta["spatial_extent"] = spatial_extent


        # export metadata to json
        export_adm_meta = adm_meta.copy()
        with open(json_path, "w") as file:
            json.dump(export_adm_meta, file, indent=4)


    def main(self):
        """
        Main function to run the geoBoundaries download process
        """
        logger = self.get_logger()

        # get the list of boundaries to download
        ingest_items = self.prepare()

        logger.info("Running boundary data download")

        # run the download tasks
        for item in ingest_items:
            logger.info(f"Processing {item[0]['boundaryISO']} boundary")
            self.dl_gb_item(item[0])

        logger.info("Finished downloading boundary data")



if __name__ == "__main__":

    config = get_config()
    boundary_config = config["boundary"]

    boundary_config["output_dir"] = Path(config["base_path"]) / "geoBoundaries"

    boundary_config["output_dir"].mkdir(parents=True, exist_ok=True)

    # set logging configuration
    logging.basicConfig(filename=boundary_config["output_dir"] / 'boundary.log',
                        filemode='a',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')


    gBD = geoBoundariesDataset(**boundary_config)
    gBD.main()
