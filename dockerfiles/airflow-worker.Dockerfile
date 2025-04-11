FROM apache/airflow:2.10.4

USER root
RUN apt-get update && apt-get install -y \
    postgis \
    postgresql-client \
    gdal-bin gdal-data gdal-plugins \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN  pip install xarray netcdf4 certifi futures geopandas h3 pygbif shapely antimeridian wget pyspark
