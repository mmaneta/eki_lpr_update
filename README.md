# Automatic Land Repurposing Program Reports (lrp_update)

This is a library to automate the production of quarterly reports for the Merced land repurposing program.
The libraries will manage the retrieval of precipitation and evapotranspiration data
from the OpenET database for the farms in the program, calculate soil water deficits and demands using an adapted
version of the SMB model, and produce a pdf report based stating if the farm is in compliance with the water use
requirements. 

A jupyter notebook is provided to facilitate the use of the library. Running the functions in the notebook will 
call the necessary functions to update the local precipitation adn evapotranspiration database 
with newly available data from OpenET and write the corresponding pdf reports.  
 

## Table of Contents

- Installation
- Usage

## Installation

How to download and install the library. We recommend and assume that python is available in your system as
distributed by **Anaconda** (https://www.anaconda.com/download).  

Step to install:

1. Step one. Create a new conda environment:

```bash
conda create -n eki_lrp_update python=3.11
conda activate eki_lrp_update         
```

2. Navigate to the working folder where you store your source code,
clone the repo and install the library:
```bash
git clone https://github.com/mmaneta/eki_lrp_update.git
cd eki_lrp_update
pip install .
```
## Usage

The easiest way to use the library to update the Merced LRP reports is to navigate to the 
`Notebooks` folder of the repo, start a jupyter notebook, and open the 
`update_lrp_merced.py` notebook:

```bash
cd notebooks
jupyter notebook update_lrp_merced.ipynb
```

Currently, the master database with the OpenEt dataset is located in the following project folder:  

`'Z:\Merced Subbasin GSA SGMA (C10076)\EKI Work Products\One-off deliverables\LRP\Enforcement Documentation\OpenET\consolidated_openet_datasets'`

This is the default folder in the notebook. If the master database is moved to a different folder, the `path_to_folder_with_data`
variable needs to be updated with the new path. 


Update the file  path to the key to access OpenET. As of now, the code ships with CHeppner's key, so please make sure
you do not commit the keys to the GitHub server.  

