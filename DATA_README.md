# NOAA Ocean Exploration Gap Analysis 
## Results Dataset

    Authors: Thomas Morrow
    Affiliations: NOAA Ocean Exploration  
    Contact: thomas.morrow@noaa.gov
    DOI: 10.5281/zenodo.15490755
    Version: v0.2.0  
    Date Published: 2025-05-23
    Repository: github.com/NOAA-OceanExploration/ExplorationGapAnalysis/

---

##  Overview

    The NOAA Ocean Exploration Gap Analysis (EGA) is a tool to establish a spatial coverage baseline for ocean exploration data holdings, support the monitoring of exploration and characterization progress on previously unexplored ocean areas, and aid in the identification of priority areas for future expeditions and data collection efforts. At its core, the EGA is a PostGIS database synthesizing deep sea scientific observations from publicly available data archives.

    This data package consists of output files in several formats described below containing the H3 Geo (h3geo.org/) hexagon addresses and corresponding exploration scores for each grid cell and observation type, as well as a copy of the PostGIS database. Users can work off of the data package results directly or clone the repository, run the analysis, and create their own results.

    The repository can be found at github.com/NOAA-OceanExploration/ExplorationGapAnalysis/

---

## Contents

    oceexp_db_dump.sql – SQL database file, entity relationships are described in the repository documentation
    h3_hexagons_03.geojson - geojson data file with level 3 H3 hexagons and corresponding EGA scores as properties
    h3_hexagons_04.geojson - geojson data file with level 4 H3 hexagons and corresponding EGA scores as properties
    h3_hexagons_05.geojson - geojson data file with level 5 H3 hexagons and corresponding EGA scores as properties
    h3_points_03.geojson - geojson data file with level 3 H3 hexagon centroid points and corresponding EGA scores as properties
    h3_points_04.geojson - geojson data file with level 4 H3 hexagon centroid points and corresponding EGA scores as properties
    h3_points_05.geojson - geojson data file with level 5 H3 hexagon centroid points and corresponding EGA scores as properties
    h3_scores_03.csv - csv data file with level 3 H3 hexagon hexadecimal indexes and columns for each corresponding EGA score type
    h3_scores_04.csv - csv data file with level 4 H3 hexagon hexadecimal indexes and columns for each corresponding EGA score type
    h3_scores_05.csv - csv data file with level 5 H3 hexagon hexadecimal indexes and columns for each corresponding EGA score type
    DATA_README.md - this readme file
    LICENSE – License information


---

## File Formats

    GeoJSON files
        
        Each GeoJSON file is formatted as a feature collection (polygon with vertices or centroid point) with the following properties:
            h3_index    - unique hexadecimal index of the H3 cell
            combined    - combined exploration score as an average of the six observation type scores for that hexagon
            mapping     - presence or absence of high resolution (multibeam) seafloor mapping data in the cell
            chemistry   - presence or absence of water chemistry observations in the cell
            geology     - presence or absence of geological samples recovered from the cell
            occurrence  - presence or absence of biological samples or observations recorded in the cell
            edna_score  - presence or absence of Environmental DNA (eDNA) sequences in the cell
            wcsd_score  - presence or absence of water column sonar observations in the cell

        Example:
            {
            "type": "FeatureCollection",
            "features": [
                {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [...
                    ]
                },
                "properties": {
                    "h3_index": "832ea9fffffffff",
                    "combined": 0.0,
                    "mapping": 0.0,
                    "chemistry": 0.0,
                    "geology": 0.0,
                    "occurrence": 0.0,
                    "edna_score": 0.0,
                    "wcsd_score": 0.0
                }
                }, . . .

    csv files

        Each csv file is formatted as comma separated list with each row containing:
            h3_index    - unique hexadecimal index of the H3 cell
            combined    - combined exploration score as an average of the six observation type scores for that hexagon
            mapping     - presence or absence of high resolution (multibeam) seafloor mapping data in the cell
            chemistry   - presence or absence of water chemistry observations in the cell
            geology     - presence or absence of geological samples recovered from the cell
            occurrence  - presence or absence of biological samples or observations recorded in the cell
            edna_score  - presence or absence of Environmental DNA (eDNA) sequences in the cell
            wcsd_score  - presence or absence of water column sonar observations in the cell
---

## Usage

    Visualization and analytical tools vary, but GeoJSON files for the hexagons are readily ingested into ArcGIS Pro using the JSON To Features tool. Higher resolution (level 6+) hexagons can be generated, but we have found that with modern computing resources, visualization begins to struggle at levels higher than 5. 