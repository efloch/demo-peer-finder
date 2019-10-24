import os, sys

MODULE_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_PATH = os.path.join(MODULE_PATH, os.pardir, "data")

import json
import requests
from collections import defaultdict

# import clean_geo
import numpy as np
import pandas as pd
from tqdm import tqdm

import src.load_data as ld

DB_URL = "http://locus-db.herokuapp.com/api/"
NAICS_DICT = {
    1998: 1997,
    1999: 1997,
    2000: 1997,
    2001: 1997,
    2002: 1997,
    2003: 2002,
    2004: 2002,
    2005: 2002,
    2006: 2002,
    2007: 2007,
    2008: 2007,
    2009: 2007,
    2010: 2007,
    2011: 2007,
    2012: 2012,
    2013: 2012,
    2014: 2012,
    2015: 2012,
    2016: 2012,
    2017: 2012,
    2018: 2012,
}


def query_fm_naics(list_of_fms, list_of_years=[2002, 2007, 2012]):
    """
    Get the unique list of naics codes across all NAICS versions.
    :param (str/list) list_of_fms: FM label/name
    :param (list) list_of_years: possible values are [1997, 2002, 2007, 2012, 2017]
    :return: (list) columns are FM labels, index/rows are NAICS codes. Values are 0 or 1.
    """
    naics_list = list()
    if isinstance(list_of_fms, str):
        list_of_fms = [list_of_fms]

    for year in list_of_years:
        print(f"getting {year} NAICS-FM values from Locus DB")
        fm_to_naics = requests.get(
            DB_URL + f"markers/crosswalk/naics/year/{year}/"
        ).json()
        for fm in list_of_fms:
            try:
                naics_list.extend(fm_to_naics.get(fm))
            except TypeError:
                print(f"Error: {fm} not found, check spelling")
    return list(set(naics_list))


def get_naics_boolean(year):
    """
    Gets the naics boolean df for a single NAICS version. If not saved in data/intermediary,
    pulls from Locus DB
    :param (int) year: possible values are [1997, 2002, 2007, 2012, 2017]
    :return: (DataFrame) columns are FM labels, index/rows are NAICS codes. Values are 0 or 1.
    """

    try:
        naics_bool = pd.read_csv(
            os.path.join(DATA_PATH, "external", f"naics{year}_fm_bool.csv"),
            dtype={"naics": str},
        )
        naics_bool.set_index("naics", inplace=True)

    except FileNotFoundError:
        print(f"getting {year} NAICS-FM boolean table from Locus DB")
        naics_bool = requests.get(
            DB_URL + f"markers/crosswalk/naics/binary/year/{year}/"
        ).json()
        naics_bool = pd.DataFrame(naics_bool).pivot(
            index="naics", columns="fm", values="value"
        )
        naics_bool.to_csv(
            os.path.join(DATA_PATH, "external", f"naics{year}_fm_bool.csv")
        )

    return naics_bool


def naics_boolean_all(list_of_years=[2002, 2007, 2012]):
    """
    Gets the naics boolean df's for a number of NAICS years and combines them (unique rows only)

    :param (list) list_of_years: possible values are [1997, 2002, 2007, 2012, 2017]
    :return: (DataFrame) columns are FM labels, index/rows are NAICS codes. Values are 0 or 1.
    """

    naics_bool_list = [get_naics_boolean(year) for year in list_of_years]
    combined_naics_bool = pd.concat(naics_bool_list)
    if combined_naics_bool.index.name == "naics":
        combined_naics_bool.reset_index(inplace=True)

    df_unique = combined_naics_bool.drop_duplicates(keep="first")

    duplicate_naics = df_unique[df_unique["naics"].duplicated()]["naics"].to_list()
    df_unique = df_unique.drop_duplicates(subset=["naics"], keep="last")

    if duplicate_naics:
        print(
            f"The following NAICS codes, {duplicate_naics} have varying "
            "barcode and FM associations in this set of crosswalks. The most "
            "recent crosswalk was chosen for these codes."
        )

    return df_unique.set_index("naics").sort_index()


def naics_data_to_fm(
    df_or_file,
    naics_bool_df=None,
    geo_var=None,
    variables=["ESTAB", "EMPL", "PAYANN"],
    outfile=None,
):
    """
    Takes YEAR x MSA x NAICS and sums across those to return a YEAR x MSA x FM table
    :param (DataFrame) df_or_file: the YEAR x MSA x NAICS dataframe or path to the file
    :param (DataFrame) naics_bool_df: if not provided, tries to read from data/intermediary or pull
                                      from LocusDB using naics_boolean_all()
    :param (str) geo_var: the column name of the geographic variable ('MSA' and 'FIPS' are checked
                                      by default
    :param (list) variables: list of variables to sum (employment, # establishments, etc.)
    :param (str) outfile: name of file to write DataFrame to (optional)
    :return: (DataFrame)
    """
    # try:
    #     df = pd.read_csv(df_or_file, dtype={'MSA':str,
    #                                         'EMPL':float,
    #                                         'ESTAB':float})
    # except FileNotFoundError as err:
    #     raise Exception(f'{filename} not found in data/processed. Please edit '\
    #                      'the filepath/filename to continue, or generate the '\
    #                      'necessary file using naics_to_fm.py') from err
    if isinstance(df_or_file, str):
        df = pd.read_csv(df_or_file)
    elif isinstance(df_or_file, pd.DataFrame):
        df = df_or_file
    else:
        raise TypeError(
            f"Filepath (str) or DataFrame required. Type {type(df_or_file)} given."
        )

    full_df = df.copy()
    full_df.columns = [col.upper() for col in df.columns]
    # check for geographic name in DataFrame columns
    if geo_var:
        pass
    elif "FIPS" in full_df.columns:
        geo_var = "FIPS"
    elif "MSA" in full_df.columns:
        geo_var = "MSA"

    assert (
        geo_var is not None
    ), "No geographic label found in columns of given DataFrame."

    if not isinstance(naics_bool_df, pd.DataFrame):
        naics_bool_df = naics_boolean_all()
    fm_dict = naics_bool_df.apply(lambda fm: list(fm[fm == 1].index)).to_dict()
    print("FM-NAICS table loaded/generated")

    processed_data = []
    for fm in tqdm(fm_dict):
        fm_subdf = (
            full_df[full_df["NAICS"].isin(fm_dict[fm])]
            .groupby(["YEAR", geo_var])[variables]
            .sum()
        )
        fm_subdf.insert(0, "FM", value=fm)
        processed_data.append(fm_subdf)
    print("Data processed into FM")

    df_out = pd.concat(processed_data).reset_index()

    if outfile:
        print("Saving file...")
        df_out.to_csv(os.path.join(DATA_PATH, "processed", outfile), index=False)
    print("\n---DONE---")
    return df_out


if __name__ == "__main__":
    county_naics_data = ld.load_by_naics(naics_level=6, geo_level="county")
    print(county_naics_data.head())
    # county_naics_data = clean_geo.relabel_legacy_msa(
    #     msa_naics_data, auto_sum=["YEAR", "NAICS"]
    # )
    outfile = "fm_by_county_all_years.csv"
    msa_fm_data = naics_data_to_fm(county_naics_data, outfile=outfile)
