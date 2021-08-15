
Tool for downloading census data for 2020 as a csv in an easy way. Might add other years if there's interest.

# Usage


To install this script run

```
pip3 install census-downloader
```

Then to run the script run

```
census-downloader --output <OUTPUT_PATH.csv> --columns <COLUMNS YOU WANT GO HERE> --states <STATES TO GO HERE>
```

You can see the spreadsheet at [this link](https://t.co/6FOEAUjOD3?amp=1) for a description of the columns.

For example, I ran

```
census-downloader --output out.csv --columns COUNTY COUSUB SUBMCD ESTATE CONCIT PLACE TRACT BLKGRP BLOCK CBSA MEMI CSA METDIV NECTA NMEMI UA UATYPE UR POP100 INTPTLAT INTPTLON P0010001 P0010002 P0010003 P0010004 P0010005 P0010006 P0010007 P0010008 P0010009 P0030001 P0030002 P0030003 P0030004 P0030005 P0030006 P0030007 P0030008 P0030009
```

Note that I didn't put a `--states` argument. By default if you don't provide it it gives you all the states

On my machine this takes 8min to run, about 8.3GB of RAM and produces a file that is 1.9GB.