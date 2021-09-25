import argparse
from .download import download_census


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="Output path (csv file)")
    parser.add_argument("--columns", nargs="+", required=True)
    parser.add_argument(
        "--states",
        nargs="+",
        default=[
            "AL",
            "AK",
            "AZ",
            "AR",
            "CA",
            "CO",
            "CT",
            "DC",
            "DE",
            "FL",
            "GA",
            "HI",
            "ID",
            "IL",
            "IN",
            "IA",
            "KS",
            "KY",
            "LA",
            "ME",
            "MD",
            "MA",
            "MI",
            "MN",
            "MS",
            "MO",
            "MT",
            "NE",
            "NV",
            "NH",
            "NJ",
            "NM",
            "NY",
            "NC",
            "ND",
            "OH",
            "OK",
            "OR",
            "PA",
            "RI",
            "SC",
            "SD",
            "TN",
            "TX",
            "UT",
            "VT",
            "VA",
            "WA",
            "WV",
            "WI",
            "WY",
            "PR",
        ],
    )
    parser.add_argument("--filter-level", default=None, type=int)
    args = parser.parse_args()
    download_census(args.columns, args.states, args.filter_level).to_csv(
        args.output, index=False
    )
