<p align="center">
  <img src="https://github.com/thomasAmorrow/oer-ega/blob/main/docs/logos/logo_banner.png?raw=true" alt="vlogo" width="800"/>
</p>

[![Version](https://img.shields.io/badge/version-0.2.0--beta-orange)]()
[![DOI](https://zenodo.org/badge/910538525.svg)](https://doi.org/10.5281/zenodo.15496931)
[![dataset DOI](https://img.shields.io/badge/DATASET%20DOI-10.5281/zenodo.15490755-blue)](https://doi.org/10.5281/zenodo.15490755)
[![NOAA Ocean Exploration](https://img.shields.io/badge/NOAA-Ocean%20Exploration-005493)](https://oceanexplorer.noaa.gov)
[![License: CC0-1.0](https://licensebuttons.net/l/zero/1.0/80x15.png)](http://creativecommons.org/publicdomain/zero/1.0/)

[![Docker](https://img.shields.io/badge/docker-28.0.4-blue?logo=docker)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.10.4-red?logo=apache-airflow)](https://airflow.apache.org/)
[![PostGIS](https://img.shields.io/badge/PostGIS-3.5.2-green?logo=postgresql)](https://postgis.net/)


##  Overview

 The **NOAA Ocean Exploration Gap Analysis (EGA)** is a tool to establish a spatial coverage baseline for ocean exploration data holdings, support the monitoring of exploration and characterization progress on previously unexplored ocean areas, and aid in the identification of priority areas for future expeditions and data collection efforts. At its core, the EGA is a PostGIS database synthesizing deep sea scientific observations from publicly available data archives.

 The current version is still in development.

## 📑 Table of Contents

- [Overview](#overview)
- [Methods and Tools](#-methods-and-tools)
- [Inputs and Outputs](#-inputs-and-outputs)
- [Installation and Usage](#-installation-and-usage)
- [Visuals](#-visuals)
- [Contributions](#-contributions)
- [Future Development](#-future-development)

## Methods and Tools

The **EGA** leverages containerized workflows, orchestrated using **Apache Airflow**, with processing steps written in **Python** and **SQL**. All tasks are managed via lightweight Airflow workers.

Spatial data are indexed using the **H3 hexagonal grid system** at resolution 5. Each hexagon receives an **Exploration Score** based on the presence or absence of observation types deeper than 200 m:

- Score **1**: Observation type is present
- Score **0**: Observation type is absent

Averaging these per observation type yields a composite score. Scores at coarser H3 resolutions (4 and 3) are computed by summing the scores of all child hexagons and dividing by the number of child hexagons (averaging).

---

## Inputs and Outputs

### Inputs

| Observation Type                             | Data Source                                                             |
|---------------------------------------------|-------------------------------------------------------------------------|
| Biological Occurrence Observations           | [GBIF](https://www.gbif.org)                                            |
| Geological Seafloor/Sub-seafloor Samples     | [NCEI Marine & Lacustrine Samples](https://www.ncei.noaa.gov/products/index-marine-lacustrine-samples) |
| Environmental DNA (eDNA) Sequences           | [OBIS](https://obis.org)                                                |
| Water Biogeochemical Samples                 | [GLODAP](https://www.glodap.info)                                       |
| Seafloor Bathymetry Coverage (ID grid)       | [GEBCO](https://www.gebco.net)                                          |
| Water Column Sonar Data                      | [NCEI Water Column Sonar](https://www.ncei.noaa.gov/products/water-column-sonar-data) |

More types are in development and will be added in future releases.

### Outputs

Output files from the full processing pipeline at hexagon resolution 5 (~11 km width). Finer resolutions (6+) can be created but are typically unweildy to analyze or visualize. Three file formats and the complete database are provided at the corresponding Zenodo Dataset: 

  - **Hexagon GeoJSON**: Full-resolution hex polygons with scores per observation type
  - **Point GeoJSON**: Lighter-weight centroid points file for each hexagon with the same properties
  - **csv**: Simple csv flat file with no geospatial data, only hexagon indices
  - **sql**: SQL dump of the entire database after assembly, cleaning, and processing

[Zenodo Dataset: DOI 10.5281/zenodo.15490755 ](https://doi.org/10.5281/zenodo.15490755)

[View PostGIS Database Entity Relationship Diagram](https://lucid.app/documents/embedded/74e51c42-7f40-4167-b0da-a49decd8267c)

---

## Installation and Usage

EGA is deployed using **Docker Compose**, currently on **Ubuntu AWS EC2**, though it's compatible with any Docker-ready environment.

- All required Dockerfiles and a `docker-compose.yml` are included
- Some paths point to an S3 bucket but can be adapted to local filesystems
- Airflow requires manual setup—see [Airflow Docs](https://airflow.apache.org/docs/)
- A sample `.env` file is included for environment configuration

---

## Visuals

<p align="center">
  <img src="https://github.com/thomasAmorrow/oer-ega/blob/main/docs/maps/ResultsMap.png?raw=true" alt="vlogo" width="600"/>
</p>

GeoJSON results files can be visualized using a number of different tools. Here in ArcGIS Pro we show the nested heirarchy structure of level 5 resolution hexagons (upper) and level 4 resolution hexagons (lower). Composite scoring in the coarser hexagons depends on their contents and completely unexplored hexagons are highlighted as the highest priority for future exploration work.

> 📌 If you generate a compelling visualization or want to share use cases, submit a PR or email us to feature it here!

---

## Contributions

This project is maintained by the **NOAA Ocean Exploration Data Lab** (Science & Technology Division) with help from the broader community.

**Interested in contributing?**  
Have data? Ideas? Feedback? Help us improve our understanding of the unknown deep ocean.

📬 Contact NOAA Ocean Exploration or open an issue/PR to get involved.

---

## Future Development

The `dev` branch is the most active—follow for updates. Upcoming milestones include:

1. DOI/citation extraction from contributing datasets
2. Leaflet-based web map viewer
3. ArcGIS-ready exports (Experience Builder, Online)
4. SME-driven enhancements to scoring methods

---

## Release Notes

**0.2.0-beta**
  - Functioning ingest from six public archives
  - Generates outputs in GeoJSON, CSV formats
  - Hosted on corresponding Zenodo Dataset
