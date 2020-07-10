# pydrip


The Dam Removal Information Portal (DRIP) is a location where partners, scientists, and practitioners can find scientific studies associated with dam removal projects. The database is intended to be regularly updated, at least annually, so that it represents the most up to date information about the scientific studies associated with dam removal. This package helps to manage the data for DIRP.

## Description


This package handles retrieval and preperation of the source data for DRIP.  Source data come from two sources.  The Dam Removal Science Database is distributed by USGS in ScienceBase (https://doi.org/10.5066/P9IGEC9G) and a complete list of dam removals is distributed by American Rivers in Figshare. We provide these efforts as a package in order to support full transparency on what we are doing with the data, to allow us to more easily update DRIP and to serve as a building block for anyone else that may want to do something similar.

### Modules

drip_sources.py : The drip_sources module contains functions that retrieve and format source data. Both sources are originally CSV files. 

drip_dam.py : The drip_dam module contains a Class Dam, allowing us to easily build an object to store information about any one given dam.  In some cases dams are in both datasets (linked by field AR_ID).  When this is the case we take information from the Dam Removal Science Database first, and fill in missing data with the American Rivers database.

drip_pipeline.py : The drip_pipeline module documents the overall pipeline that uses the other modules to retrieve and process data so that it is ready for use in DRIP.



## Dependencies


The package uses some basic Python tools in Python 3.x and above along with the following specific dependencies:




It is recommended that you set up a discrete Python environment for this project using your tool of choice. The install_requires section of the setup.py should create your dependencies for you on install. You can install from source with a local clone or directly from the source repo with...

``pip install git+git://github.com/usgs-biolab/pydrip.git@main``

...or...

``pip install git+git://github.com/usgs-biolab/pydrip.git@develop``

...for the latest.



## Provisional Software Statement


Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.