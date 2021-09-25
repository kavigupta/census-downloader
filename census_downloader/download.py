import io
import warnings
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


def download_census_for_state(state, columns, filter_level):
    assert set(columns) & {"CHARITER", "CIFSN", "FILEID"} == set()
    geoheaders, segment_headers = get_headers()
    s_name = state.name.replace(" ", "_")
    s_abbr = state.abbr.lower()
    with urlopen(f"{PREFIX}/{s_name}/{s_abbr}2020.pl.zip") as f:
        result = f.read()
    f = zipfile.ZipFile(io.BytesIO(result))

    def collect(path, headers):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
            result = pd.read_csv(
                io.BytesIO(f.read(path)),
                sep="|",
                names=headers,
                encoding="latin-1",
            )
            result = result[
                [
                    col
                    for col in result
                    if col in columns or col == "LOGRECNO" or col == "SUMLEV"
                ]
            ]
            return result

    geodb = collect(f"{s_abbr}geo2020.pl", geoheaders)
    seps = {
        i: collect(f"{s_abbr}0000{i}2020.pl", segment_headers[i])
        for i in segment_headers
    }
    tables = [geodb, *[seps[i] for i in sorted(seps)]]
    overall = tables[0]
    for table in tables[1:]:
        common_columns = set(table) & set(overall)
        for col in common_columns:
            assert (table[col] == overall[col]).all()
        overall = overall.merge(table)

    if filter_level is not None:
        overall = overall[overall.SUMLEV == filter_level]

    return overall[columns]


def download_census(columns, states, filter_level):
    result = None
    for state in tqdm.tqdm(us.states.STATES_AND_TERRITORIES + [us.states.DC]):
        if state.abbr not in states:
            continue
        current = download_census_for_state(
            state, columns=columns, filter_level=filter_level
        )
        if result is None:
            result = current
        else:
            result = pd.concat([result, current])
    return result
