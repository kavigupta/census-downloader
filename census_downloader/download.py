import io
import warnings
import zipfile
from urllib.request import urlopen

import us
import tqdm
import pandas as pd
from permacache import permacache


PREFIX = "https://www2.census.gov/programs-surveys/decennial/{year}/data/01-Redistricting_File--PL_94-171"

headers_2020 = "https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/2020_PLSummaryFile_FieldNames.xlsx"


def get_headers(year):
    # assuming 2010 has the same headers as 2020 execpt without the 3rd sheet

    assert year == 2020

    xls = pd.ExcelFile(headers_2020)
    geoheaders = list(pd.read_excel(xls, "2020 P.L. Geoheader Fields"))
    segment_headers = {
        i: list(pd.read_excel(xls, f"2020 P.L. Segment {i} Fields")) for i in (1, 2, 3)
    }
    return geoheaders, segment_headers


PER_FILE_COLUMNS = {"FILEID", "CHARITER", "CIFSN"}


def download_census_for_state(state, columns, *, filter_level, year):
    geoheaders, segment_headers = get_headers(year)
    if columns is None:
        columns = geoheaders + [v for vals in segment_headers.values() for v in vals]
        columns = [x for x in columns if x not in PER_FILE_COLUMNS]
    assert set(columns) & PER_FILE_COLUMNS == set()
    s_name = state.name.replace(" ", "_")
    s_abbr = state.abbr.lower()
    with urlopen(f"{PREFIX.format(year=year)}/{s_name}/{s_abbr}{year}.pl.zip") as f:
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
            # import IPython; IPython.embed()
            result = result[
                [
                    col
                    for col in result
                    if col in columns or col == "LOGRECNO" or col == "SUMLEV"
                ]
            ]
            return result

    geodb = collect(f"{s_abbr}geo{year}.pl", geoheaders)
    seps = {
        i: collect(f"{s_abbr}0000{i}{year}.pl", segment_headers[i])
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
