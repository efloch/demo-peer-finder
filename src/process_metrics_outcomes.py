import pandas as pd
import os

import os, sys
import requests
import pandas as pd
from src.load_data import *

# import clean_geo
import src.naics_to_fm


# all_df = []
# PATH = "../data/SummerDataPackage/functional/geo/county/"
# for subdir, dirs, files in os.walk(PATH):
#     for file in files:
#         filepath = subdir + os.sep + file
#         df = pd.read_csv(filepath)
#         df = df[["FIPS", "YEAR", "NAICS_LEVEL", "NAICS", "EMPL", "PAYANN", "ESTAB"]]
#         print(df.head())
#         df["AREA"] = df["FIPS"]
#         df.drop(["FIPS"], 1, inplace=True)
#         all_df.append(df)
#
# df_all = pd.concat(all_df)
# df_all.to_csv("../data/metrics.csv", index=False)


"""
This script creates CSV files containing 22 metrics for industry
presence within communities for several years
Right now the wrapper is only supported for MSA. Need to change the
get_naics to allow for county data to be pulled from load_data()
"""

MODULE_PATH = os.path.dirname(os.path.realpath(__file__))
DATA_PATH = os.path.join(MODULE_PATH, os.pardir, "data")
#
# if MODULE_PATH not in sys.path:
#     sys.path.append(MODULE_PATH)
#

METRICS = {
    "lq": "Location Quotient",
    "pc": "Proportion of Community",
    "cm": "Cutoff Matrix",
    "adj_cm": "Adjusted Cutoff Matrix",
    "ms": "Weighted Matrix",
    "eer": "Employment Establishment Ratio",
    "bm": "Binarized LQ",
    "bm_combo": "Binarized LQ Weighted",
    "lq_combo": "Location Quotient Weighted",
}

DTYPE_DICT = {
    "MSA": str,
    "FIPS": str,
    "FIPS_ST": str,
    "CBSA_CODE": str,
    "NAICS": str,
    "YEAR": int,
}

## HELPER FUNCTIONS ##
def _compute_totals(df, df_naics, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Given a Dataframe by FMs, and a DataFrame
    by NAICS codes, saves columns for total counts: Total
    sum for a given year, total sum per msa for a given year,
    and total sum per function for a given year..
    """
    print("Computing functional, geographical, and national totals...")

    for var in variables:
        # Calculates national total
        df[f"TOTAL_{var}"] = df["YEAR"].map(df_naics.groupby("YEAR")[var].sum())

        # Calculates community totals
        df_geo_totals = (
            df_naics.groupby(["YEAR", geo])[var]
            .sum()
            .reset_index(name=f"TOTAL_{var}_{geo}")
        )
        df = df.merge(df_geo_totals, on=[geo, "YEAR"])

        # Calculates FM totals
        df_fm_totals = (
            df.groupby(["YEAR", "FM"])[var].sum().reset_index(name=f"TOTAL_{var}_FM")
        )
        df = df.merge(df_fm_totals, on=["YEAR", "FM"])
    return df


def _compute_local_totals(df, df_naics, local, geo="MSA", variables=["EMPL", "ESTAB"]):
    """
    Given a Dataframe by FMs, a DataFrame by NAICS codes,
    and a list of local NAICS codes or FMs,
    saves columns for local function sums for a given year,
    total sum per msa for a given year,
    and total sum per function for a given year..
    """
    print("Computing geographical and national local function totals...")

    if isinstance(local, list):
        if not local[0].isdigit():
            local = naics_to_fm.query_fm_naics(local)
    ## create subset dataframe with just the local function rows
    df_naics_local = df_naics[df_naics["NAICS"].isin(local)]

    for var in variables:
        # Calculates national total of local functions
        df[f"LOCAL_{var}"] = df["YEAR"].map(df_naics_local.groupby("YEAR")[var].sum())

        # Calculates community total of local functions
        tot_geo_local = df_naics_local.groupby(["YEAR", geo])[var].sum()
        df = df.merge(
            tot_geo_local.reset_index(name=f"LOCAL_{var}_{geo}"), on=[geo, "YEAR"]
        )

    return df


def _get_naics(geo="FIPS", filepath=None, relabel_msa=True):
    """
    Get NAICS dataframe needed for computing totals columns.
    If filepath is not provided, a default file path is used.
    :param geo:
    :param filepath: (str) full file path to the existing naics data
                     or otherwise where it should be saved.
    """
    if not filepath:
        filepath = os.path.join(
            DATA_PATH, "intermediary", geo, "naics_6digit_by_msa.csv"
        )

    if os.path.isfile(filepath):
        return pd.read_csv(filepath, dtype=DTYPE_DICT)
    else:
        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        # Naics DataFrame required to calculate total counts of variables
        df_all_naics = load_by_naics(naics_level=6, geo_level="county")
        if geo == "msa" and relabel_msa:
            df_all_naics = clean_geo.relabel_legacy_msa(
                df_all_naics, auto_sum=["YEAR", "NAICS"]
            )

        df_all_naics.to_csv(filepath, index=False)

        return df_all_naics


def _test_normality(fm_df, metric, summary=True):
    """
    Tests the following distributional statistics for a given metric:
        - whether, on average, the mean and median are roughly the same
        - whether, on average, min and max are equidistant from the mean
        - whether there is high variance in the max of each FM
        - what % of the FMs had a p value < 0.05 for a normality test
    """
    test = lambda series: stats.kstest(series, cdf="norm")[1]
    df = fm_df.groupby("FM")[metric].agg(["min", "mean", "median", "max", test])
    if summary:
        med_max = (df["mean"] - df["median"]).mean()
        min_max = (
            (df["min"] - df["mean"]).abs() - (df["max"] - df["mean"]).abs()
        ).mean()
        max = df["max"].std() / df["max"].mean()
        norm = sum(df["<lambda>"] < 0.05) / len(df)
        return {"med_max": med_max, "balance": min_max, "max": max, "norm": norm}
    return df


def _get_function(metric):
    """
    Return function that calculates desired metric.
    :param metric: (str) all, combo, derived, or single metric from METRICS.
    """
    if metric == "all":
        return yearly_func_dist
    elif metric == "combo":
        return combo_metrics
    elif metric == "derived":
        return derived_metrics
    elif metric == "core":
        return core_metrics
    else:
        for key, value in globals().items():
            if key == f"calc_{metric}" and value.__module__ == __name__:
                return value

    return None


## METRIC CALCULATIONS ##
def calc_lq(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate the location quotient. Requires proportion of community,
    total count for the functional groups and the total counts of the
    community. Will call dependencies where possible, apart from total
    counts, which needs to be calculated based on NAICS codes rather than
    functional groups due to lack of mutual exclusivity in markers.
    """
    for var in variables:
        try:
            df[f"LQ_{var}"] = df[f"PC_{var}"] / (
                df[f"TOTAL_{var}_FM"] / df[f"TOTAL_{var}"]
            )
        except KeyError as err:
            if str(err) == f"'PC_{var}'":
                df = calc_pc(df, geo, variables)
                df = calc_lq(df, geo, variables)
            elif "TOTAL" in str(err):
                if geo == "MSA":
                    df_naics = _get_naics()
                    df = _compute_totals(df, df_naics, geo, variables)
                    df = calc_lq(df, geo, variables)
                else:
                    raise KeyError(
                        "Totals need to be computed before computing metrics. "
                        "NAICS data required because of mutual exclusivity. Pass "
                        "a NAICS file into _compute_totals() to continue."
                    )
            else:
                raise

    return df


def calc_nq(df, local, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate the nonlocal quotient, which is similar to the location quotient but
    uses as denominator the local function economic output instead of total economic output.
    Requires proportion of community, total count for the functional groups and
    the total counts of the community.
    """
    for var in variables:
        try:
            df[f"NQ_{var}"] = (df[f"{var}"] / df[f"LOCAL_{var}_{geo}"]) / (
                df[f"TOTAL_{var}_FM"] / df[f"LOCAL_{var}"]
            )
        except KeyError as err:
            if "LOCAL" in str(err):
                df_naics = _get_naics()
                df = _compute_local_totals(df, df_naics, local)
                df = calc_nq(df, local, geo, variables)

            elif "TOTAL" in str(err):
                if geo == "MSA":
                    df_naics = _get_naics()
                    df = _compute_totals(df, df_naics, geo, variables)
                    df = calc_nq(df, local, geo, variables)
                else:
                    raise KeyError(
                        "Totals need to be computed before computing metrics. "
                        "NAICS data required because of mutual exclusivity. Pass "
                        "a NAICS file into _compute_totals() to continue."
                    )
            else:
                raise

    return df


def calc_pc(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate the FM's proportion of community total.
    """
    for var in variables:
        try:
            df[f"PC_{var}"] = df[f"{var}"] / df[f"TOTAL_{var}_{geo}"]
        except KeyError as err:
            if str(err) == f"'{var}'":
                print(
                    f"{var} not found in DataFrame. Add {var} column, "
                    "or modify variables to continue."
                )
            elif "TOTAL" in str(err):
                if geo == "MSA":
                    df_naics = _get_naics()
                    df = _compute_totals(df, df_naics, geo, variables)
                    df = calc_pc(df, geo, variables)
                else:
                    print(
                        "Totals need to be computed before computing metrics. "
                        "NAICS data required because of mutual exclusivity. Pass "
                        "a NAICS file into _compute_totals() to continue."
                    )
    return df


def calc_ms(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate the market share (commmunity's proportion of FM).
    """
    for var in variables:
        try:
            df[f"MS_{var}"] = df[f"{var}"] / df[f"TOTAL_{var}_FM"]
        except KeyError as err:
            if str(err) == f"'{var}'":
                print(
                    f"{var} not found in DataFrame. Add {var} column, "
                    "or modify variables to continue."
                )
            elif "TOTAL" in str(err):
                if geo == "MSA":
                    df_naics = _get_naics()
                    df = _compute_totals(df, df_naics, geo, variables)
                    df = calc_ms(df, geo, variables)
                else:
                    print(
                        "Totals need to be computed before computing metrics. "
                        "NAICS data required because of mutual exclusivity. Pass "
                        "a NAICS file into _compute_totals() to continue."
                    )

            else:
                raise

    return df


def calc_eer(df, geo="FIPS"):
    """
    Calculate employee-establishment ratio and relative to national
    """
    try:
        df["EER"] = df["EMPL"] / df["ESTAB"]
        df["REL_EER"] = df["EER"] / (df["TOTAL_EMPL_FM"] / df["TOTAL_ESTAB_FM"])
    except KeyError as err:
        if str(err) == "'EMPL'":
            print(
                f"EMPL not found in DataFrame. Add {var} column, "
                "or modify variables to continue."
            )
        elif str(err) == "'ESTAB'":
            print(
                f"ESTAB not found in DataFrame. Add {var} column, "
                "or modify variables to continue."
            )
        elif "TOTAL" in str(err):
            if geo == "MSA":
                df_naics = _get_naics()
                df = _compute_totals(df, df_naics, geo, variables)
                df = calc_eer(df, geo, variables)
            else:
                print(
                    "Totals need to be computed before computing metrics. "
                    "NAICS data required because of mutual exclusivity. Pass "
                    "a NAICS file into _compute_totals() to continue."
                )
        else:
            raise

    return df


def calc_pres(df, geo="FIPS", variables=["EMPL", "ESTAB"], min_threshold=0):
    """
    Calculate presence, i.e. if nonzero establishments or employees exist.
    EMPL is the more stringent criteria. PRES_ESTAB can be > 1 with PRES_EMPL == 0,
    but if PRES_EMPL > 0, PRES_ESTAB must be > 0. To account for estimation uncertainty and
    data suppression, set threshold to be higher than 0.
    """
    for var in variables:
        try:
            df[f"PRES_{var}"] = df[f"{var}"] > min_threshold
            df[f"PRES_{var}"].astype(int)
        except KeyError as err:
            print(f'Column "{var}" cannot be found')

    return df


def calc_cm(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate cutoff matrix metric.
    """
    for var in variables:
        try:
            cutoff = (df[f"BM_{var}"] > 1) | (df[var] > 50)

        except KeyError as err:
            if str(err) == f"'BM_{var}'":
                df = calc_bm(df, geo, variables)
                df = calc_cm(df, geo, variables)
    df[f"CM_{var}"] = cutoff.astype(int)

    return df


def calc_bm(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculate binary metric based on location quotient.
    """
    for var in variables:
        try:
            df[f"BM_{var}"] = df[f"LQ_{var}"] > 1
            df[f"BM_{var}"] = df[f"BM_{var}"].astype(int)
        except KeyError as err:
            if str(err) == f"'LQ_{var}'":
                df = calc_lq(df, geo, variables)
                df = calc_bm(df, geo, variables)

    return df


def calc_adj_cm(df, geo="FIPS", variables=["EMPL", "ESTAB"], cutoff=0.01):
    """
    This function computes the adjusted (by proportion) cutoff metric for establishment/employment.
    If LQ > 1 or if the industry makes up more than X% (default is 1%) of the community's total,
    then the cutoff metric = 1.
    """

    for var in variables:
        try:
            df[f"ADJ_CM_{var}"] = df.apply(
                lambda row: (row[f"BM_{var}"] >= 1) or (row[f"PC_{var}"] >= cutoff),
                axis=1,
            )
            df[f"ADJ_CM_{var}"] = df[f"ADJ_CM_{var}"].astype(int)

        except KeyError as err:
            if f"BM_{var}" in str(err):
                df = calc_bm(df, geo, variables)
                df = calc_adj_cm(df, geo, variables, cutoff)
            elif f"PC_{var}" in str(err):
                df = calc_pc(df, geo, variables)
                df = calc_adj_cm(df, geo, variables, cutoff)

    return df


def calc_bm_combo(df, geo="FIPS", weighting={"EMPL": 0.5, "ESTAB": 0.5}):
    """
    Calculate binary metric based on location quotient.
    """
    try:
        df["BM_EMPL_ESTAB"] = df["LQ_EMPL_ESTAB"].apply(lambda lq: 1 if lq > 1 else 0)
    except KeyError as err:
        if str(err) == f"'LQ_EMPL_ESTAB'":
            df = calc_lq_combo(df, geo, weighting)
            df = calc_bm_combo(df, geo, weighting)

    return df


def calc_lq_combo(df, geo="FIPS", weighting={"EMPL": 0.5, "ESTAB": 0.5}):
    """
    Calculate weighted location quotient.
    """
    try:
        df["LQ_EMPL_ESTAB"] = (df["LQ_ESTAB"] * weighting["ESTAB"]) + (
            df["LQ_EMPL"] * weighting["EMPL"]
        )
    except KeyError as err:
        if str(err) == "'LQ_ESTAB'":
            df = calc_lq(df, geo, variables=["ESTAB"])
            df = calc_lq_combo(df, geo, weighting)
        elif str(err) == "'LQ_EMPL'":
            df = calc_lq(df, geo, variables=["EMPL"])
            df = calc_lq_combo(df, geo, weighting)

    return df


def standardize_log(fm_df, metric="LQ", variables=["EMPL", "ESTAB"], ensure=True):
    """
    Transform a metric into log, standardized, and standardized log forms
    in order to address normality issues. See Tian (2013) Measuring Agglomeration...
    If ensure=True, adds 0.0001 to series so log does not result in -inf values.
    """
    zscore = lambda series: (series - series.mean()) / series.std()

    for var in variables:
        if ensure:
            fm_df["temp_metric"] = fm_df[f"{metric}_{var}"] + 0.0001
        else:
            fm_df["temp_metric"] = fm_df[f"{metric}_{var}"]

        fm_df[f"L{metric}_{var}"] = fm_df.groupby("FM")["temp_metric"].apply(np.log)
        fm_df[f"Z{metric}_{var}"] = fm_df.groupby("FM")[f"temp_metric"].apply(zscore)
        fm_df[f"S{metric}_{var}"] = fm_df.groupby("FM")[f"L{metric}_{var}"].apply(
            zscore
        )
        fm_df.drop(["temp_metric"], axis=1, inplace=True)
    return fm_df


def bootstrap_cutoff(series, percentile=0.95, n_iter=1000):
    """
    Use bootstrap sampling to estimate the true 95th percentile cutoff value
    Then returns binary values for each row depending on whether it surpasses that cutoff
    """
    sample_cutoffs = []
    for i in range(n_iter):
        sample_cutoffs.append(
            series.sample(n=len(series), replace=True).quantile(q=percentile)
        )
    cutoff = np.mean(sample_cutoffs)
    return (series > cutoff).astype(int)


def fill_zero_pivot(gbdf, geo="FIPS"):
    """
    Adds zeros to metrics that were previously filtered out
    """
    assert len(gbdf["YEAR"].unique()) == 1, "DataFrame contains more than one year"

    all_combo = [(msa, fm) for msa in gbdf[geo].unique() for fm in gbdf["FM"].unique()]
    missing = list(set(all_combo) - set(list(zip(gbdf[geo], gbdf["FM"]))))

    columns = [
        var
        for var in gbdf.columns
        if var not in ["YEAR", geo, "FM", "FM_LEVEL", f"{geo}_NAME"]
    ]

    na_data = pd.DataFrame(
        {
            **{geo: list(list(zip(*missing))[0]), "FM": list(list(zip(*missing))[1])},
            **{var: 0 for var in columns},
        }
    )

    na_data.insert(loc=0, column="YEAR", value=gbdf["YEAR"].unique()[0])
    df = pd.concat([gbdf, na_data], sort=False)

    return df


def get_ranks(
    fm_df, rank_metrics=["LQ_EMPL", "LQ_ESTAB", "LQ_EMPL_ESTAB"], ascending=False
):
    """
    Calculates MSA ranks for a set of defined metrics in each FM.
    """

    for metric in rank_metrics:
        fm_df[metric + "_RANK"] = fm_df.groupby("FM")[metric].rank(ascending=ascending)
    return fm_df


## WRAPPER FUNCTIONS ##
def combo_metrics(df, geo="FIPS", weighting={"EMPL": 0.5, "ESTAB": 0.5}):
    """
    Calculates
        1. Weighted location quotient
        2. Binarized weighted location quotient
        3. Presence in weighted location quotient
        5. Calculates absolute employee:establishment ratio and relative to national
    Arguments:
        df: DataFrame with EMPL and ESTAB counts for a single year
    """
    df = calc_lq_combo(df, geo, weighting)
    df = calc_bm_combo(df, geo, weighting)
    df = calc_eer(df, geo)

    return df


def derived_metrics(df, geo="FIPS", variables=["EMPL", "ESTAB"], cutoff=0.01):
    """
    Calculates
        1. binary
        2. cutoff
        3. adjusted (proportional) cutoff
        4. presence
    Arguments:
        df: DataFrame with EMPL and/or ESTAB counts for a given year
        variables: variables for which to calculate metrics (EMPL, ESTAB, PAYANN..)
        geo: geographic level (MSA, COUNTY)
        cutoff: presence cutoff threshold used to calculate adjusted cm metric
    """
    df = calc_bm(df, geo, variables)
    df = calc_cm(df, geo, variables)
    df = calc_adj_cm(df, geo, variables, cutoff)
    df = calc_pres(df, geo, variables)

    return df


def core_metrics(df, geo="FIPS", variables=["EMPL", "ESTAB"]):
    """
    Calculates
        1. PC_EMPL, PC_ESTAB proportion of community total
        2. MS_EMPL, MS_ESTAB proportion of national industry (market share)
        3. LQ_EMPL, LQ_ESTAB location quotient
        4. EER employee:establishment ratio
    Arguments:
        df: DataFrame with EMPL and/or ESTAB counts for a given year by functional
                 groupings
        df_naics: DataFrame with EMPL and/or ESTAB counts for a given year by NAICS codes
        variables: variables for which to calculate metrics (EMPL, ESTAB, PAYANN..)
        geo: geographic level (MSA, COUNTY)
    """
    df = calc_pc(df, geo, variables)
    df = calc_ms(df, geo, variables)
    df = calc_lq(df, geo, variables)

    return df


def yearly_func_dist(
    df,
    geo="FIPS",
    variables=["EMPL", "ESTAB"],
    weighting={"EMPL": 0.5, "ESTAB": 0.5},
    cutoff=0.01,
):
    """
    Calculates all new metrics for a given year of FM x MSA data
    """
    # Calculate the location quotient, market share, and proportion of community for
    # both establishment and employment
    df = core_metrics(df, geo, variables)
    # Calculate the presence and binarized metrics for establishment and employment
    df = derived_metrics(df, geo, variables, cutoff)
    # Calculate metrics based on employee AND establishment LQ
    df = combo_metrics(df, geo, weighting)

    # Drop all unnecessary columns
    df.drop(
        [col for col in df.columns if col.startswith("TOTAL")], axis=1, inplace=True
    )

    # Add ranks
    df = get_ranks(df)

    return df


def func_dist_wrapper(
    fm_df_or_file,
    df_all_naics=None,
    metric="all",
    geo="FIPS",
    variables=["EMPL", "ESTAB"],
    include_zero=True,
    outfile_path=None,
    years=None,
    weighting={"EMPL": 0.5, "ESTAB": 0.5},
    cutoff=0.01,
):
    """
    Wrapper to generate all functional presence metrics for all years.
    """
    if isinstance(fm_df_or_file, str):
        try:
            df = pd.read_csv(fm_df_or_file, dtype=DTYPE_DICT)
        except FileNotFoundError as err:
            raise Exception(
                f"{filename} not found in data/processed. Please edit "
                "the filepath/filename to continue, or generate the "
                "necessary file using naics_to_fm.py"
            ) from err
    elif isinstance(fm_df_or_file, pd.DataFrame):
        df = fm_df_or_file
    else:
        raise TypeError(
            f"Filename (str) or DataFrame required. Type "
            f"{type(rm_df_or_file)} given."
        )

    if not years:
        years = df["YEAR"].unique()

    df = df[df["YEAR"].isin(years)]

    metric_function = _get_function(metric)
    # Naics DataFrame required to calculate total counts of variables
    if not isinstance(df_all_naics, pd.DataFrame):
        df_all_naics = _get_naics()

    df = _compute_totals(df, df_all_naics, geo, variables)

    final_df = []

    for year, df_year in df.groupby("YEAR"):
        # We first subset the dataframe to only include one year
        # df_year = df[df["YEAR"]==year].copy()

        default_kwargs = {
            "df": df_year,
            "geo": geo,
            "variables": variables,
            "weighting": weighting,
            "cutoff": cutoff,
        }
        kwargs = {
            key: default_kwargs[key]
            for key in default_kwargs
            if key in metric_function.__code__.co_varnames
        }
        # for kwarg in kwargs:
        #     if kwarg not in metric_function.__code__.co_varnames:
        #         kwargs.pop(kwargs)

        df_metric = metric_function(**kwargs)

        if include_zero:
            df_metric = fill_zero_pivot(df_metric, geo)

        print(f"{year} complete.")

        if outfile_path:
            df_metric.to_csv(
                os.path.join(
                    os.path.dirname(outfile_path), f"fm_by_{geo}_metrics_{year}.csv"
                )
            )
        final_df.append(df_metric)

    final_df = pd.concat(final_df)
    final_df.drop(
        [col for col in final_df.columns if col.startswith("TOTAL")],
        axis=1,
        inplace=True,
    )

    if outfile_path:
        print("saving file...")
        final_df.to_csv(outfile_path, index=False)

    print("Successfully generated functional presence metrics")
    return final_df


if __name__ == "__main__":
    # years = [2015]
    # # Edit to appropriate path to file containing FM x MSA x YEAR data file
    # filename = "fm_by_county_all_years.csv"
    # path_to_file = os.path.join(DATA_PATH, "processed", filename)
    #
    # # Edit to appropriate outfile name and path placement
    outfile = "fm_by_county_metrics.csv"
    path_to_outfile = os.path.join(DATA_PATH, "processed", outfile)
    #
    # df_metrics = func_dist_wrapper(
    #     path_to_file, outfile_path=path_to_outfile, years=[2015, 2016]
    # )

    yearly_df = []
    fm_df = pd.read_csv(path_to_outfile)
    vars_of_interest = ["PC_EMPL", "PRES_ESTAB", "LQ_EMPL_ESTAB_RANK"]
    for year in [2016]:
        sub_dfs = []
        fm_df = fm_df.query("YEAR==@year")
        for vari in vars_of_interest:
            temp_df = fm_df[["FIPS", "YEAR", "FM", vari]]
            temp_df = temp_df.pivot(index="FIPS", columns="FM", values=vari)
            temp_df = temp_df.add_suffix("-" + vari)
            print(temp_df.head())
            sub_dfs.append(temp_df)
        yearly = pd.concat(sub_dfs, axis=1)
        yearly.reset_index()
        yearly_df.append(yearly)
    df_county = pd.concat(yearly_df)
    df_county.reset_index(inplace=True)
    df_county.to_csv(os.path.join(DATA_PATH, "processed", "metrics_county.csv"))
    acs_county = pd.read_csv("data/SummerDataPackage/outcome/acs_cleaned_county.csv")
    metrics_outcomes = df_county.merge(df_county, on="FIPS")
    metrics_outcomes.to_csv("data/processed/county_metrics_outcomes.csv")
