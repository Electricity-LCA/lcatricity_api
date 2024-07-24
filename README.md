LCAtricity API
===============

This repository contains the source code for the API serving LCAtricity.

# Multi-Impact Electricity modelling using live data sources
Estimate real-time environmental impacts using electricity generation data reported by national and super-national bodies

# Set up
1. Create an ENTSO-E account and request an API key
2. Decide on a place to run a database and [setup postgres](https://www.postgresql.org/docs/current/tutorial-install.html). 
3. Create an empty database called `electricity_lca` and create a user account with privilege to create tables on the database
4. Clone this repository
5. Copy `template_project.env` to a new file `.env` and fill the copied file with `ENTSOE_SECURITY_TOKEN` = your ENTSOE security token. Fill the connection details to your postgres sql instance
6. Create virtual environment
```commandline
venv create elec_lca_venv
```
7. Install requirements
```commandline
pip install -r requirement.txt
```
7. Run `python src/setup/setup.py` to initalize the database schema and load static data 
8. Run all tests under `tests/`

# Set up database
Run 
```commandline
python src/setup/setup.py
```
> This creates the tables in the database and fills the constant value (e.g. environmental impacts, regions, electricity generation types)
> using the data in the `data/` directory


# Documentation
See the documentation [here](electricity-lca.github.io)

The raw files can be found in the [docs](/docs) directory in this repository

# Updating the docs
```bash
cd docs
make clean html
```
(or use make.bat on windows)

Export the docker image:
```bash
docker image save --output lcatricity_api_0_1_0.tar lcatricity_api-lcatricity_api:latest
```

# Set up on Raspberry Pi
Tested on a Raspberry Pi 4, with 32 GB SD card (use a good one)

Trying to install on solar power on a cloudy day is not likely to work. Use a battery pack :)

1. Get Docker
```bash
curl -sSL https://get.docker.com | sh
sudo usermod -aG docker $USER
```