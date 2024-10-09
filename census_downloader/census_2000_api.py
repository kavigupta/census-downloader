import us
import requests
import pandas as pd
from permacache import permacache, stable_hash

from census_downloader.census_2000_intpt import (
    compute_intpts_for_state,
    county_folders,
    state_folders,
    didnt_exist_in_2000,
)

# no_data = [
#     "60_AMERICAN_SAMOA",
#     "66_GUAM",
#     "69_COMMONWEALTH_OF_THE_NORTHERN_MARIANA_ISLANDS",
#     "72_PUERTO_RICO",
#     "78_VIRGIN_ISLANDS_OF_THE_UNITED_STATES",
# ]

# see racial columns: https://www.socialexplorer.com/data/C2000/metadata/?ds=SF1&table=P008
column_remap_2020_to_2000 = {
    # all pop
    "POP100": "P001001",
    # population over 18
    "P0030001": "P005001",
    # hispanic
    "P0020002": "P008010",
    # white
    "P0020005": "P008003",
    # black
    "P0020006": "P008004",
    # native
    "P0020007": "P008005",
    # asian
    "P0020008": "P008006",
    # hawaiian/pi
    "P0020009": "P008007",
    # other
    "P0020010": "P008008",
    # mixed
    "P0020011": "P008009",
    # total
    "H0010001": "H003001",
    # occupied
    "H0010002": "H003002",
    # vacant
    "H0010003": "H003003",
}
column_remap_2000_to_2020 = {v: k for k, v in column_remap_2020_to_2000.items()}


@permacache(
    "census_downloader/census_2000_api/get_for_county_2",
    key_function=dict(cols_for_query=stable_hash),
    multiprocess_safe=True,
)
def get_for_county(state_fips, county_fips, cols_for_query):
    cols_for_query_2010 = [column_remap_2020_to_2000[x] for x in cols_for_query]
    url = "https://api.census.gov/data/2000/dec/sf1?" + "&".join(
        [
            "get=" + ",".join(cols_for_query_2010),
            "for=block:*",
            f"in=state:{state_fips}",
            f"in=county:{county_fips}",
            "key=4e82296adc802cf0ed80f04c83c1f12e626d49af",
        ]
    )
    table = requests.get(url).json()
    table = pd.DataFrame(
        table[1:], columns=[column_remap_2000_to_2020.get(x, x) for x in table[0]]
    )
    for k in table:
        if k in column_remap_2020_to_2000:
            table[k] = table[k].astype(int)
    return table


def data_for_county(geoid_to_idx, coords, state_folder, county_folder, columns):
    cols_for_query = [
        x for x in columns if x not in ["INTPTLAT", "INTPTLON", "SUMLEV", "GEOID"]
    ]
    table = get_for_county(
        state_folder.split("_")[0], county_folder.split("_")[0][2:], cols_for_query
    )
    table = table.copy()
    geoid_short = (
        table.state
        + table.county
        + table.tract.apply(lambda x: x + "00" if len(x) == 4 else x)
        + table.block
    )
    if "GEOID" in columns:
        table["GEOID"] = "7500000US" + geoid_short
    if "SUMLEV" in columns:
        table["SUMLEV"] = 750
    if "INTPTLAT" in columns or "INTPTLON" in columns:
        table[["INTPTLON", "INTPTLAT"]] = coords[
            geoid_short.apply(int).apply(lambda x: geoid_to_idx[x])
        ]
    return table[columns]


def data_for_state_2000(state, columns):
    [state_folder] = [
        x
        for x in state_folders()
        if x[3:].lower().replace("_", " ") == state.name.lower()
    ]
    coords, geoids = compute_intpts_for_state(state_folder)
    geoid_to_idx = {int(x): i for i, x in enumerate(geoids)}
    frames = []
    for county_folder in county_folders(state_folder):
        if (state_folder, county_folder) in didnt_exist_in_2000:
            continue
        frames.append(
            data_for_county(geoid_to_idx, coords, state_folder, county_folder, columns)
        )
    return pd.concat(frames)
