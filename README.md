# pydrip


The Dam Removal Information Portal (DRIP) is a location where partners, scientists, and practitioners can find scientific studies associated with dam removal projects. The database is intended to be regularly updated, at least annually, so that it represents the most up to date information about the scientific studies associated with dam removal. This package helps to manage the data for DIRP.

## Description


This package handles getting the source data for DRIP which comes from two sources.  The Dam Removal Science Database is distributed by USGS in ScienceBase (https://doi.org/10.5066/P9IGEC9G) and a complete list of dam removals is distributed by American Rivers in Figshare. We provide it as a package in order to support full transparency on what we are doing with the data and to serve as a building block for anyone else that may want to do something similar.

The core drip module contains functions that process source data. Both sources are originally CSV files. In this case no transformations to the data are required.

The core functions of the package include the following:

* 



## Dependencies


The package uses some basic Python tools in Python 3.x and above along with the following specific dependencies:



It is recommended that you set up a discrete Python environment for this project using your tool of choice. The install_requires section of the setup.py should create your dependencies for you on install. You can install from source with a local clone or directly from the source repo with...

``pip install git+git://github.com/usgs-bcb/pydrip.git@master``

...or...

``pip install git+git://github.com/usgs-bcb/pydrip.git@develop``

...for the latest.



## Provisional Software Statement


Under USGS Software Release Policy, the software codes here are considered preliminary, not released officially, and posted to this repo for informal sharing among colleagues.

This software is preliminary or provisional and is subject to revision. It is being provided to meet the need for timely best science. The software has not received final approval by the U.S. Geological Survey (USGS). No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.