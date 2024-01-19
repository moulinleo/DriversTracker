# DriversTracker
DriverTrackers is a project aiming at tracking the drivers using Python, Dask and pandas.

## Description
This project involves building a data processing pipeline to analyze the driving habits of individuals based on GPS data collected from mobile phones. The data includes information about the position of each driver, obtained from the GPS unit, and is stored in a CSV file (drivers.csv). Each row in the CSV represents a GPS measurement and includes details such as the driver's unique identifier, timestamp, latitude, and longitude coordinates.

## Table of Contents
- [Installation](#installation)
- [Structure](#structure)
- [Usage](#usage)

## Installation
To be able to run the project, you need to install the following dependencies:  
`pip install -r requirements.txt`


## Structure
The project is structured as follows:
- `dataset` folder contains the data used in the project (hidden in Github)
- `notebooks` folder contains the main notebook used in the project
- `src` folder contains the source code of the project
    - `data_cleaning` folder contains the data cleaning steps
    - `data_loading` folder contains the data loading
    - `data_processing` folder contains the main computations steps
    - `geo_visualization` folder contains the geographical visualization functions
    - `utils` folder contains some utility functions
    - `visualization_analysis` contains the visialization functions for the analysis part

## Usage
To start the project, open the `main_notebook.ipynb` file and run the cells in order.




