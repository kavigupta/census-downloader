import io
import warnings
import zipfile
from urllib.request import urlopen

import us
import tqdm
import pandas as pd
from permacache import permacache

from census_downloader.census_2000_api import data_for_state_2000

geoheaders_2010 = {
    "SUMLEV": [(8, 11)],
    "LOGRECNO": [(18, 25)],
    "GEOID": [(27, 32), (54, 65)],
    "POP100": [(318, 327)],
    "INTPTLAT": [(336, 347)],
    "INTPTLON": [(347, 359)],
}

PREFIX = "https://www2.census.gov/programs-surveys/decennial/{year}/data/01-Redistricting_File--PL_94-171"

headers_2020 = "https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020_PLSummaryFile_FieldNames.xlsx"


def get_headers(year):
    if year == 2010:
        _, segment_headers_2020 = get_headers(2020)
        return geoheaders_2010, {1: segment_headers_2020[1], 2: segment_headers_2020[2]}

    xls = pd.ExcelFile(headers_2020)
    geoheaders = list(pd.read_excel(xls, "2020 P.L. Geoheader Fields"))
    segment_headers = {
        i: list(pd.read_excel(xls, f"2020 P.L. Segment {i} Fields")) for i in (1, 2, 3)
    }
    return geoheaders, segment_headers


PER_FILE_COLUMNS = {"FILEID", "CHARITER", "CIFSN"}


def read_2010_geo(data, headers):
    data = data.decode("latin-1").split("\n")
    assert data.pop() == ""
    columns = {}
    for header, ranges in headers.items():
        columns[header] = []
        for line in data:
            assert line
            columns[header].append("".join(line[start:end] for start, end in ranges))
        if header == "GEOID":
            columns[header] = ["7500000US" + x for x in columns[header]]
        elif header == "LOGRECNO":
            columns[header] = [int(x.strip()) for x in columns[header]]
        else:
            columns[header] = [float(x.strip()) for x in columns[header]]
    table = pd.DataFrame(columns)
    return table


def download_census_for_state(state, columns, *, filter_level, year):
    assert filter_level == 750
    if year == 2000:
        return data_for_state_2000(state, columns)
    geoheaders, segment_headers = get_headers(year)
    if columns is None:
        columns = geoheaders + [v for vals in segment_headers.values() for v in vals]
        columns = [x for x in columns if x not in PER_FILE_COLUMNS]
    assert set(columns) & PER_FILE_COLUMNS == set()
    s_name = state.name.replace(" ", "_")
    s_abbr = state.abbr.lower()
    zip_path = f"{PREFIX.format(year=year)}/{s_name}/{s_abbr}{year}.pl.zip"
    with urlopen(zip_path) as f:
        result = f.read()
    f = zipfile.ZipFile(io.BytesIO(result))

    def collect(path, headers, is_geo=False):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)
            if year == 2010 and is_geo:
                return read_2010_geo(f.read(path), headers)
            result = pd.read_csv(
                io.BytesIO(f.read(path)),
                sep="|" if year == 2020 else ",",
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

    geodb = collect(f"{s_abbr}geo{year}.pl", geoheaders, is_geo=True)
    seps = {
        i: collect(f"{s_abbr}0000{i}{year}.pl", segment_headers[i])
        for i in segment_headers
    }
    tables = [geodb, *[seps[i] for i in sorted(seps)]]
    overall = tables[0]
    for table in tables[1:]:
        common_columns = set(table) & set(overall)
        for col in common_columns:
            assert (table[col] == overall[col]).all(), (col, table[col], overall[col])
        overall = overall.merge(table)

    if filter_level is not None:
        overall = overall[overall.SUMLEV == filter_level]

    return overall[columns]


def download_census(columns, states, filter_level, *, year):
    result = None
    for state in tqdm.tqdm(us.states.STATES_AND_TERRITORIES + [us.states.DC]):
        if state.abbr not in states:
            continue
        current = download_census_for_state(
            state, columns=columns, filter_level=filter_level, year=year
        )
        if result is None:
            result = current
        else:
            result = pd.concat([result, current])
    return result.sort_values("SUMLEV")
