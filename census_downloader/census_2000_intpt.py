import re
import time
import numpy as np
import requests
import tqdm.auto as tqdm

from permacache import permacache
import geopandas as gpd

ftp_site = "https://www2.census.gov/geo/tiger/TIGER2007FE/"
didnt_exist_in_2000 = [("08_COLORADO", "08014_Broomfield")]


def state_folders():
    response = requests.get(ftp_site)
    # extract <tr><td valign="top"><img src="/icons/folder.gif" alt="[DIR]"></td><td><a href="47_TENNESSEE/">47_TENNESSEE/</a></td><td align="right">2008-03-26 15:56  </td><td align="right">  - </td><td>&nbsp;</td></tr>
    # only want the 47_TENNESSEE part, and only if it starts with ##
    state_pattern = re.compile(
        r'<tr><td valign="top"><img src="/icons/folder.gif" alt="\[DIR\]"></td><td><a href="(\d\d_.+?)/">'
    )
    return state_pattern.findall(response.text)


def county_folders(state_folder):
    # like the above but the counties look like 01001_Autauga
    response = requests.get(ftp_site + state_folder)
    county_pattern = re.compile(
        r'<tr><td valign="top"><img src="/icons/folder.gif" alt="\[DIR\]"></td><td><a href="(\d\d\d\d\d_.+?)/">'
    )
    return county_pattern.findall(response.text)


def files_in_folder(state_folder, county_folder):
    response = requests.get(ftp_site + state_folder + "/" + county_folder)
    # <tr><td valign="top"><img src="/icons/compressed.gif" alt="[   ]"></td><td><a href="fe_2007_01001_facesal.zip">fe_2007_01001_facesal.zip</a></td><td align="right">2008-03-27 14:20  </td><td align="right">3.7K</td><td>&nbsp;</td></tr>
    file_pattern = re.compile(
        r'<tr><td valign="top"><img src="/icons/compressed.gif" alt="\[   \]"></td><td><a href="(.+?\.zip)">'
    )
    return file_pattern.findall(response.text)


def compute_tabblock(state_folder, county_folder):
    tabblock00 = [
        f for f in files_in_folder(state_folder, county_folder) if "tabblock00" in f
    ]
    if not tabblock00:
        return None
    [file] = tabblock00
    url = ftp_site + state_folder + "/" + county_folder + "/" + file
    return url


class CountyNotFound(Exception):
    def __init__(self, state_folder, county_folder):
        self.state_folder = state_folder
        self.county_folder = county_folder

    def __str__(self):
        return f"County not found: {self.state_folder}/{self.county_folder}"


@permacache(
    "census_downloader/census_2000_intpt/compute_intpt_for_county",
    multiprocess_safe=True,
)
def compute_intpt_for_county(state_folder, county_folder):
    url = compute_tabblock(state_folder, county_folder)
    if url is None:
        raise CountyNotFound(state_folder, county_folder)
    while True:
        try:
            shapefile = gpd.read_file(url)
            break
        except Exception as e:
            print(e)
            print("Retrying in 5 seconds...")
            time.sleep(5)
    intpt = shapefile.representative_point()
    xy = np.array([intpt.x, intpt.y]).T
    p = np.array(shapefile.BLKIDFP00.apply(int))
    return xy, p


@permacache(
    "census_downloader/census_2000_intpt/compute_intpts_for_state",
    multiprocess_safe=True,
)
def compute_intpts_for_state(state_folder):
    cfs = county_folders(state_folder)
    results = []
    for county_folder in tqdm.tqdm(cfs, desc=state_folder):
        try:
            results.append(compute_intpt_for_county(state_folder, county_folder))
        except CountyNotFound:
            if (state_folder, county_folder) in didnt_exist_in_2000:
                continue
            raise
    xys, ps = zip(*results)
    return np.concatenate(xys), np.concatenate(ps)


@permacache(
    "census_downloader/census_2000_intpt/compute_intpts",
    multiprocess_safe=True,
)
def compute_intpts():
    results = []
    for state_folder in tqdm.tqdm(state_folders()):
        results.append(compute_intpts_for_state(state_folder))
    xys, ps = zip(*results)
    return np.concatenate(xys), np.concatenate(ps)
