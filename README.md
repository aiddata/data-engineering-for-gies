# Data Engineering for GIEs

Examples and excercises for the "Practical Data Engineering for GIEs" chapter of the Geospatial Impact Evaluation textbook.

## Setting up the environment

1. [Install uv](https://docs.astral.sh/uv/getting-started/installation/)

2. In your terminal, clone this repository and use the `cd` command to navigate into it
   ```
   git clone git@github.com:aiddata/data-engineering-for-gies.git
   cd cd data-engineering-for-gies
   ```

3. Run `uv sync`

4. Set your Copernicus Climate Data Store API key to run the 2_landcover.py script
   - Register an account and log in to the Copernicus Climate Data Store (https://cds.climate.copernic
   - Go to your profile and the licenses page (https://cds.climate.copernicus.eu/profile?tab=licences) and agree to the following licenses:
      - ESA CCI licence
      - VITO licence
      - Licence to use Copernicus Products
   - Go back to your main profile page and scroll down to generate/copy your API key.
   - Save the API key to a “.env” file in your project directory with the contents as displayed below.
      - `CDS_API_KEY="<YOUR-CDS-API-KEY>"`
      - In the terminal where you will be running the scripts, run `export UV_ENV_FILE=.env`

5. Run example scripts using `uv run [SCRIPT]`, e.g. `uv run 1_boundary.py`
