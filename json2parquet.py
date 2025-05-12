import geopandas as gpd
from shapely.geometry import shape
import shapely.wkt

def round_coords(geom, precision=5):
    # Applies rounding via WKT (Well-Known Text)
    if geom is None or geom.is_empty:
        return geom
    return shapely.wkt.loads(shapely.wkt.dumps(geom, rounding_precision=precision))


for i in ["03", "04", "05"]:
    # Load hex GeoJSON
    gdf = gpd.read_file(f"h3_hexagons_{i}.geojson")

    # Round geometry coordinate precision
    gdf["geometry"] = gdf["geometry"].apply(lambda g: round_coords(g, precision=5))

    # Save as GeoParquet with brotli compression
    gdf.to_parquet(f"h3_hexagons_{i}.parquet", index=False, compression="brotli", compression_level=11)

    # Load point GeoJSON
    gdf = gpd.read_file(f"h3_points_{i}.geojson")

    # Save as GeoParquet with brotli compression
    gdf.to_parquet(f"h3_points_{i}.parquet", index=False, compression="brotli", compression_level=11)