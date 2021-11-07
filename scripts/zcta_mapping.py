import geopandas as gpd
import json

census2020 = load_blocks("/home/kavi/temp/census_2020.csv")[
    [
        "GEOID",
        "INTPTLAT",
        "INTPTLON",
    ]
]
# from https://www2.census.gov/geo/tiger/TIGER2021/ZCTA520/
zcta = gpd.read_file("/home/kavi/Downloads/zctas/tl_2021_us_zcta520.shp")
points = [
    geometry.point.Point(*z)
    for z in tqdm.tqdm(np.array(census2020[["INTPTLON", "INTPTLAT"]]))
]
census2020 = gpd.GeoDataFrame(census2020, geometry=points)
census2020 = census2020.set_crs(epsg=4326)
zcta = zcta.to_crs(epsg=4326)
block_to_zcta = gpd.sjoin(census2020, zcta, how="left", op="intersects")
filtered_blocks = block_to_zcta[block_to_zcta.ZCTA5CE20 == block_to_zcta.ZCTA5CE20]
# prints 99.97208664879254
print(filtered_blocks.shape[0] / census2020.shape[0] * 100)
block_map = dict(zip(filtered_blocks.GEOID, filtered_blocks.ZCTA5CE20))

with open("outputs/block_to_zcta_2020.json", "w") as f:
    json.dump(block_map, f, indent=2)