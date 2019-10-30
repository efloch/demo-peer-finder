from ipywidgets import interactive, interact, fixed, interact_manual, Layout
import ipywidgets as widgets
import sys
import locuspeerexplorer.peer_finder as find
import locuspeerexplorer.peer_explorer as exp
import locuspeerexplorer.peer_visualizer as vis
import locuspeerexplorer.params as param
import pandas as pd
import os
import itertools

# import geoviz.choropleth as choro

df_data = pd.read_csv("data/processed/metrics_outcomes.csv")
df_msa_def = pd.read_csv("data/external/omb_msa_1990_2018.csv")
df_county_dist = pd.read_csv("data/external/sf12010countydistance500miles.csv")


def code2name(code):
    code = int(code)
    return list(df_msa_def[df_msa_def["CBSA_CODE"] == code].head(1).CBSA_TITLE)[0]


def name2code(name):
    df_name = df_msa_def[df_msa_def["CBSA_TITLE"].str.contains(name)]
    return list(df_name.head(1).CBSA_CODE)[0]


def pretty_prints(peers, fms):
    print("--------------------")
    print("| Peers identified |")
    print("--------------------")
    for i in [code2name(x) for x in peers]:
        print(i)
    print("-------------------")
    print("|  Features used  |")
    print("-------------------")
    for f in fms:
        print(f)


pop_limit = 0.2

all_msas = df_msa_def.set_index("CBSA_TITLE").to_dict()["CBSA_CODE"]

all_fms = {
    " ".join((c.split("-")[0]).split("_")): c.split("-")[0]
    for c in df_data.columns
    if "PC_EMPL" in c
}
# all_fms["None"] = None
all_outcomes = {c: c for c in list(df_data.columns)[3:] if ("-" not in c)}
# all_outcomes["None"] = None

MSA = "New York"


style = {"description_width": "initial"}

input_n_peers = widgets.IntSlider(
    value=5, min=0, max=20, step=1, description="# of Peers", orientation="horizontal"
)
input_n_features = widgets.IntSlider(
    value=5,
    min=0,
    max=30,
    step=1,
    description="# of Features",
    orientation="horizontal",
)
input_n_fms = widgets.IntSlider(
    value=5, min=0, max=30, step=1, description="# of FMs", orientation="horizontal"
)
input_coverage = widgets.IntSlider(
    value=50, min=0, max=100, step=1, description="% of Empl", orientation="horizontal"
)
input_year = widgets.RadioButtons(
    options=[2015, 2016], description="Year", disabled=False
)
input_msa = widgets.Dropdown(
    options=all_msas,
    value=name2code(MSA),
    description="MSA",
    layout=Layout(width="80%"),
)
input_fms = widgets.SelectMultiple(
    options=all_fms, description="FM(s)", layout=Layout(width="80%")
)

input_fms_agg = widgets.SelectMultiple(
    options=param.FM_DICT["AgricultureFishingForestry"],
    description="FM(s) Aggriculture",
    layout=Layout(width="80%"),
    rows=3,
    style=style,
)

input_fms_biz = widgets.SelectMultiple(
    options=param.FM_DICT["BusinessServices"],
    description="FM(s) Business Services",
    layout=Layout(width="80%"),
    rows=3,
    style=style,
)

# input_fms_cons,
# # input_fms_en,
# # input_fms_fin,
# # input_fms_food,
# # input_fms_health,
# # input_fms_infra,
# # input_fms_manu,
# # input_fms_media,
# # input_fms_min,
# # input_fms_serv,
# # input_fms_real,
# # input_fms_retail,
# # input_fms_tech,
# # input_fms_transp,

accordion = widgets.Accordion(children=[input_fms_agg, input_fms_biz])
# input_fms_cons,
# input_fms_en,
# input_fms_fin,
# input_fms_food,
# input_fms_health,
# input_fms_infra,
# input_fms_manu,
# input_fms_media,
# input_fms_min,
# input_fms_serv,
# input_fms_real,
# input_fms_retail,
# input_fms_tech,
# input_fms_transp])
accordion.set_title(0, "Business services")

input_outcomes = widgets.SelectMultiple(
    options=all_outcomes, description="Outcome(s)", layout=Layout(width="80%")
)

input_population = widgets.Checkbox(
    value=False, description="Add population", disabled=False
)


def show_peers(df_data, df_county_dist, df_msa_def, msa, n_peers, year):
    peers, fms = find.get_geographic_peers(
        df_data, df_county_dist, df_msa_def, msa, n_peers, year
    )
    #     peers.append(msa)
    df_peers = pd.DataFrame({"MSA": [str(x) for x in peers]})
    df_peers["Peer MSA Code"] = df_peers["MSA"].astype(str)
    df_peers["Peer Name"] = df_peers["MSA"].apply(code2name)
    df_peers["Is peer"] = 1
    #     choro.plot(
    #         df_peers,
    #         "MSA",
    #         "cbsa",
    #         "Is peer",
    #         "sequential",
    #         formatting={"state_outline": "after"},
    #     )

    return df_peers[["Peer Name", "Peer MSA Code"]]


def show_fms_peers(
    df_data,
    msa,
    year,
    n_peers,
    fms,
    # fms_biz,
    #                    fms_cons,
    #                    fms_en,
    #                    fms_fin,
    #                    fms_food,
    #                    fms_health,
    #                    fms_infra,
    #                    fms_manu,
    #                    fms_media,
    #                    fms_min,
    #                    fms_serv,
    #                    fms_real,
    #                    fms_retail,
    #                    fms_tech,
    #                    fms_transp,
    outcomes,
):
    # fms = list(itertools.chain(fms_agg, fms_biz))
    if outcomes == "None" or outcomes == [None]:
        outcomes = []
    else:
        outcomes = list(outcomes)

    if fms == "None" or fms == [None]:
        fms = []
    else:
        fms = list(fms)
    peers, fms = find.get_peers_from_input(df_data, msa, year, n_peers, fms, outcomes)
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, msa, peers, fms)
    for i in fms:
        vis.duo_fm_viz(df_data, msa, [msa] + peers, i, save_fig=False, show=True)
    df_peers = pd.DataFrame({"Peer MSA Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer MSA Code"].apply(code2name)
    return df_peers


def show_disting_peers(df_data, msa, year, n_peers, n_feat, filter_pop):
    if filter_pop:
        filter_pop = pop_limit
    else:
        filter_pop = None
    peers, fms = find.get_distinguishing_features_peers(
        df_data, msa, year, n_peers, n_feat, filter_pop
    )
    print(f"Comparison of {msa} and its peers for the {n_feat} most distinguishing FMs")
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, msa, peers, fms)
    for i in fms:
        vis.duo_fm_viz(df_data, msa, [msa] + peers, i, save_fig=False, show=True)
    df_peers = pd.DataFrame({"Peer MSA Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer MSA Code"].apply(code2name)
    return df_peers


def show_top_fms_peers(df_data, msa, year, n_peers, n_fms, filter_pop):
    if filter_pop:
        filter_pop = pop_limit
    else:
        filter_pop = None
    peers, fms = find.get_top_n_fms_peers(
        df_data, msa, year, n_peers, n_fms, filter_pop
    )
    print(f"Comparison of {msa} and its peers for the {n_fms} most present FMs")
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, msa, peers, fms)
    for i in fms:
        vis.duo_fm_viz(df_data, msa, [msa] + peers, i, save_fig=False, show=True)
    df_peers = pd.DataFrame({"Peer MSA Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer MSA Code"].apply(code2name)
    return df_peers


def show_coverage_peers(df_data, msa, year, n_peers, coverage,  filter_pop):
    coverage = coverage / 10
    if filter_pop:
        filter_pop = pop_limit
    else:
        filter_pop = None
    peers, fms = find.get_emp_threshold_peers(
        df_data, msa, year, n_peers, coverage, filter_pop
    )
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, msa, peers, fms)
    for i in fms:
        vis.duo_fm_viz(df_data, msa, [msa] + peers, i, save_fig=False, show=True)
    df_peers = pd.DataFrame({"Peer MSA Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer MSA Code"].apply(code2name)
    return df_peers
