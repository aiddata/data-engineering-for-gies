from pathlib import Path
from typing import Union

import tomllib


def get_config(config_path: Union[Path, str] = "config.toml"):
    """
    Load a TOML configuration file.

    Args:
        config_path (Union[Path, str]): Path to the TOML configuration file.

    Returns:
        dict: Configuration data loaded from the TOML file.
    """
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path, "rb") as src:
            return tomllib.load(src)
    else:
        return FileNotFoundError("No TOML config file found for dataset.")
