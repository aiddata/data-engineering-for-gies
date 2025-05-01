"""
Download and prepare land cover raster data from ESA

https://cds.climate.copernicus.eu/datasets/satellite-land-cover
"""

import logging
import os
import shutil
from pathlib import Path
from typing import List
from zipfile import ZipFile

import cdsapi
import numpy as np
import rasterio

from config import get_config

config = get_config()


class ESALandcover:
    name = "ESA Landcover"

    def __init__(
        self,
        raw_dir: str,
        process_dir: str,
        output_dir: str,
        years: List[int],
        api_key: str,
        overwrite_download: bool,
        overwrite_processing: bool,
        mapping: dict,
    ):
        self.raw_dir = Path(raw_dir)
        self.process_dir = Path(process_dir)
        self.output_dir = Path(output_dir)
        self.years = years
        self.api_key = api_key
        self.overwrite_download = overwrite_download
        self.overwrite_processing = overwrite_processing

        self.v207_years = range(1992, 2016)
        self.v211_years = range(2016, 2022)

        cdsapi_path = Path.home() / ".cdsapirc"
        with open(cdsapi_path, "w") as f:
            f.write(
                f"url: https://cds.climate.copernicus.eu/api \nkey: {self.api_key}"
            )

        self.cdsapi_client = cdsapi.Client()

        vector_mapping = {
            int(vi): int(k) for k, v in mapping.items() for vi in v
        }
        self.map_func = np.vectorize(vector_mapping.get)

    def get_logger(self):
        """
        Retrieve and return the base logger to be used for this dataset
        """
        return logging.getLogger("dataset")

    def download(self, year: int):
        """Download the ESA land cover data for a given year"""
        logger = self.get_logger()

        if year in self.v207_years:
            version = "v2_0_7cds"
        elif year in self.v211_years:
            version = "v2_1_1"
        else:
            version = "v2_1_1"
            logger.warning(f"Assuming that {year} is v2_1_1")

        dl_path = self.raw_dir / "compressed" / f"{year}.zip"
        print(dl_path)

        if not dl_path.exists() or self.overwrite_download:
            dl_meta = {
                "variable": "all",
                "format": "zip",
                "version": [version],
                "year": [str(year)],
            }
            self.cdsapi_client.retrieve(
                "satellite-land-cover", dl_meta, dl_path
            )

        zipfile_path = dl_path.as_posix()

        logger.info(f"Unzipping {zipfile_path}...")

        with ZipFile(zipfile_path) as zf:
            netcdf_namelist = [i for i in zf.namelist() if i.endswith(".nc")]
            if len(netcdf_namelist) != 1:
                raise Exception(
                    f"Multiple or no ({len(netcdf_namelist)}) net cdf files found in zip for {year}"
                )
            output_file_path = (
                self.raw_dir / "uncompressed" / netcdf_namelist[0]
            )
            if not os.path.isfile(output_file_path) or self.overwrite_download:
                zf.extract(netcdf_namelist[0], self.raw_dir / "uncompressed")
                logger.info(f"Unzip complete: {zipfile_path}...")
            else:
                logger.info(f"Unzip exists: {zipfile_path}...")

        return output_file_path

    def process(self, input_path: Path, output_path: Path):
        logger = self.get_logger()

        if self.overwrite_download and not self.overwrite_processing:
            logger.warning(
                "Overwrite download set but not overwrite processing."
            )

        if output_path.exists() and not self.overwrite_processing:
            logger.info(f"Processed layer exists: {input_path}")

        else:
            logger.info(f"Processing: {input_path}")

            self.process_dir.mkdir(parents=True, exist_ok=True)

            tmp_input_path = self.process_dir / Path(input_path).name
            tmp_output_path = self.process_dir / Path(output_path).name

            logger.info(f"Copying input to tmp {input_path} {tmp_input_path}")
            shutil.copyfile(input_path, tmp_input_path)

            logger.info(
                f"Running raster calc {tmp_input_path} {tmp_output_path}"
            )
            netcdf_path = f"netcdf:{tmp_input_path}:lccs_class"

            default_meta = {
                "driver": "COG",
                "compress": "LZW",
                # 'count': 1,
                # 'crs': {'init': 'epsg:4326'},
                # 'nodata': -9999,
            }

            with rasterio.open(netcdf_path) as src:
                assert len(set(src.block_shapes)) == 1
                meta = src.meta.copy()
                meta.update(**default_meta)
                with rasterio.open(tmp_output_path, "w", **meta) as dst:
                    for ji, window in src.block_windows(1):
                        in_data = src.read(window=window)
                        out_data = self.map_func(in_data)
                        out_data = out_data.astype(meta["dtype"])
                        dst.write(out_data, window=window)

            logger.info(
                f"Copying output tmp to final {tmp_output_path} {output_path}"
            )
            shutil.copyfile(tmp_output_path, output_path)

        return

    def main(self):
        """
        Main function to run the ESA land cover data download and processing
        """
        logger = self.get_logger()

        os.makedirs(self.raw_dir / "compressed", exist_ok=True)
        os.makedirs(self.raw_dir / "uncompressed", exist_ok=True)

        # Download data
        logger.info("Running data download")

        download_results = []
        for year in self.years:
            download = self.download(year)
            download_results.append(download)

        os.makedirs(self.output_dir, exist_ok=True)

        # Process data
        logger.info("Running processing")

        process_inputs = zip(
            download_results,
            [self.output_dir / f"esa_lc_{year}.tif" for year in self.years],
        )

        for pi in process_inputs:
            _ = self.process(*pi)

        logging.info("Finished processing land cover data")


if __name__ == "__main__":
    config = get_config()
    base_path = Path(config["base_path"])
    lc_config = {
        "raw_dir": base_path / config["landcover"]["dataset_name"] / "tmp/raw",
        "process_dir": base_path
        / config["landcover"]["dataset_name"]
        / "tmp/processed",
        "output_dir": base_path / config["landcover"]["dataset_name"],
        "years": config["landcover"]["years"],
        "api_key": os.environ[config["landcover"]["api_key_env_var"]],
        "overwrite_download": config["landcover"]["overwrite_download"],
        "overwrite_processing": config["landcover"]["overwrite_processing"],
        "mapping": config["landcover"]["mapping"],
    }

    lc_config["output_dir"].mkdir(parents=True, exist_ok=True)

    # set logging configuration
    logging.basicConfig(
        filename=lc_config["output_dir"] / "dataset.log",
        filemode="a",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    ESA_LC = ESALandcover(**lc_config)
    ESA_LC.main()
