import io
import zipfile
from urllib.request import urlopen

import us
import tqdm
import pandas as pd
from permacache import permacache


PREFIX = "https://www2.census.gov/programs-surveys/decennial/2020/data/01-Redistricting_File--PL_94-171"


def get_headers():
    xls = pd.ExcelFile("https://t.co/6FOEAUjOD3?amp=1")
    geoheaders = list(pd.read_excel(xls, "2020 P.L. Geoheader Fields"))
    segment_headers = {
        i: list(pd.read_excel(xls, f"2020 P.L. Segment {i} Fields")) for i in (1, 2, 3)
    }
    return geoheaders, segment_headers


@permacache(
    "census-centroid-blocks/download_census_for_state_3",
    key_function=dict(state=lambda state: state.abbr),
)
def download_census_for_state(state, columns):
    geoheaders, segment_headers = get_headers()
    s_name = state.name.replace(" ", "_")
    s_abbr = state.abbr.lower()
    with urlopen(f"{PREFIX}/{s_name}/{s_abbr}2020.pl.zip") as f:
        result = f.read()
    f = zipfile.ZipFile(io.BytesIO(result))
    geodb = pd.read_csv(
        io.BytesIO(f.read(f"{s_abbr}geo2020.pl")),
        sep="|",
        names=geoheaders,
        encoding="latin-1",
    )
    seps = {
        i: pd.read_csv(
            io.BytesIO(f.read(f"{s_abbr}0000{i}2020.pl")),
            sep="|",
            names=segment_headers[i],
            encoding="latin-1",
        )
        for i in segment_headers
    }
    remove_columns = ["CHARITER", "CIFSN", "FILEID"]
    tables = [geodb, *[seps[i] for i in sorted(seps)]]
    tables = [table[[c for c in table if c not in remove_columns]] for table in tables]
    overall = tables[0]
    for table in tables[1:]:
        common_columns = set(table) & set(overall)
        for col in common_columns:
            assert (table[col] == overall[col]).all()
        overall = overall.merge(table)

    return overall[columns]


@permacache("census-centroid-blocks/download_census")
def download_census(columns):
    result = {}
    for state in tqdm.tqdm(us.states.STATES_AND_TERRITORIES):
        try:
            result[state.abbr] = download_census_for_state(state, columns=columns)
        except Exception as e:
            print("INVALID", state, e)
    return pd.concat(result.values())
