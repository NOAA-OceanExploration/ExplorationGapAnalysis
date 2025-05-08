import geopandas as gpd

for i in ["03", "04"]:
    # Load GeoParquet
    gdf = gpd.read_parquet(f"h3_hexagons_{i}.parquet")

    # Save as GeoJSON
    gdf.to_file(f"h3_hexagons_{i}.geojson", driver="GeoJSON")

    gdf = gpd.read_parquet(f"h3_points_{i}.parquet")

    # Save as GeoJSON
    gdf.to_file(f"h3_points_{i}.geojson", driver="GeoJSON")