from ipywidgets import interactive, interact, fixed, interact_manual, Layout
import sys
import locuspeerexplorer.peer_finder as find
import locuspeerexplorer.peer_explorer as exp
import locuspeerexplorer.peer_visualizer as vis
import pandas as pd
import os
import geoviz.choropleth as choro

df_data = pd.read_csv(
    "/Users/efloch/Locus/peer-explorer/data/processed/metrics_outcomes.csv"
)
df_msa_def = pd.read_csv(
    "/Users/efloch/Locus/peer-explorer/data/external/omb_msa_1990_2018.csv"
)
df_county_dist = pd.read_csv(
    "/Users/efloch/Locus/peer-explorer/data/processed/sf12010countydistance500miles.csv"
)


def code2name(code):
    code = int(code)
    return list(df_msa_def[df_msa_def["CBSA_CODE"] == code].head(1).CBSA_TITLE)[0]


def name2code(name):
    df_name = df_msa_def[df_msa_def["CBSA_TITLE"].str.contains(name)]
    return list(df_name.head(1).CBSA_CODE)[0]


all_msas = df_msa_def.set_index("CBSA_TITLE").to_dict()["CBSA_CODE"]
all_fms = {
    " ".join((c.split("-")[0]).split("_")): c.split("-")[0]
    for c in df_data.columns
    if "PC_EMPL" in c
}
all_fms["None"] = None
all_outcomes = {c: c for c in list(df_data.columns)[3:] if ("-" not in c)}
all_outcomes["None"] = None


def show_peers(df_data, df_county_dist, df_msa_def, msa, n_peers, year):
    peers, fms = find.get_geographic_peers(
        df_data, df_county_dist, df_msa_def, msa, n_peers, year
    )
    peers.append(msa)
    df_peers = pd.DataFrame({"MSA": [str(x) for x in peers]})
    df_peers["MSA"] = df_peers["MSA"].astype(str)
    df_peers["Name"] = df_peers["MSA"].apply(code2name)
    df_peers["Is peer"] = 1
    choro.plot(
        df_peers,
        "MSA",
        "cbsa",
        "Is peer",
        "sequential",
        formatting={"state_outline": "after"},
    )

    return df_peers


def show_fms_peers(df_data, msa, year, n_peers, fms, outcomes):
    if fms == "None":
        fms = []
    else:
        fms = list(fms)
    if outcomes == "None":
        outcomes = []
    else:
        outcomes = list(outcomes)
    peers, fms = find.get_peers_from_input(df_data, msa, year, n_peers, fms, outcomes)
    for i in fms:
        vis.quadrant_viz(df_data, msa, [msa] + peers, i, save_fig=False, show=True)
    df_peers = pd.DataFrame({"MSA": [str(x) for x in peers]})
    df_peers["Name"] = df_peers["MSA"].apply(code2name)
    return df_peers


def show_disting_peers(df_data, msa, year, n_peers, n_feat):
    peers, fms = find.get_distinguishing_features_peers(
        df_data, msa, year, n_peers, n_feat
    )
    print(f"Comparison of {msa} and its peers for the 5 most distinguishing FMs")
    for i in range(0, 5):
        vis.quadrant_viz(df_data, msa, [msa] + peers, fms[i], save_fig=False, show=True)
    df_peers = pd.DataFrame({"MSA": [str(x) for x in peers]})
    df_peers["Name"] = df_peers["MSA"].apply(code2name)
    return df_peers


def show_top_fms_peers(df_data, msa, year, n_peers, n_fms):
    peers, fms = find.get_top_n_fms_peers(df_data, msa, year, n_peers, n_fms)
    print(f"Comparison of {msa} and its peers for the 5 most present FMs")
    for i in range(0, 5):
        vis.quadrant_viz(df_data, msa, [msa] + peers, fms[i], save_fig=False, show=True)
    df_peers = pd.DataFrame({"MSA": [str(x) for x in peers]})
    df_peers["Name"] = df_peers["MSA"].apply(code2name)
    return df_peers
