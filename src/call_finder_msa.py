from ipywidgets import interactive, interact, fixed, interact_manual, Layout
import ipywidgets as widgets
import sys
import locuspeerexplorer.peer_finder as find
import locuspeerexplorer.peer_explorer as exp
import locuspeerexplorer.peer_visualizer as vis
import locuspeerexplorer.params as param
import pandas as pd
import us
import os
import itertools

# import geoviz.choropleth as choro

df_data = pd.read_csv("data/processed/metrics_outcomes.csv")
df_data.dropna(how='all', axis=1, inplace=True)
df_msa_def = pd.read_csv("data/external/omb_msa_1990_2018.csv")
df_county_dist = pd.read_csv("data/external/sf12010countydistance500miles.csv")


def get_data(area):
    area = int(area)
    if area in list(df_data['AREA']):
        return df_data


def code2name(code):
    code = int(code)
    if code in list(df_data['AREA']):
        return df_data[df_data["AREA"] == code].AREA_NAME.iloc[0]


def name2code(name):
    if name.isin(df_msa_def['CBSA_TITLE']):
        return df_msa_def[df_msa_def["CBSA_TITLE"] == name].CBSA_CODE.iloc[0]


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
        if '-' in f:
            fm = f.split('-')[0]
            fm = fm.replace('_', ' ')
            fm = fm.capitalize()
            metric = f.split('-')[1]
            if metric == 'PC_EMPL':
                metric = 'Concentration'
            elif metric.startswith('LQ'):
                metric = 'Location quotient'
            print(f"{fm} ({metric})")
        else:
            print(f.replace('_', ' ').capitalize())


all_areas = df_data.set_index("AREA_NAME").to_dict()["AREA"]

all_fms = {
    " ".join((c.split("-")[0]).split("_")): c.split("-")[0]
    for c in df_data.columns
    if "PC_EMPL" in c
}


# all_fms = {" ".join(c.split("_")).capitalize():c for k in param.FM_DICT for c in param.FM_DICT[k]}

all_outcomes = {c: c for c in list(df_data.columns)[3:] if ("-" not in c)}
# print([x.replace('_', ' ').capitalize() for x in list(all_outcomes.keys())])
# all_outcomes["None"] = None

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
    value=5, min=0, max=30, step=1, description="# of Industry", orientation="horizontal"
)
input_coverage = widgets.IntSlider(
    value=50, min=0, max=100, step=1, description="% of Empl", orientation="horizontal"
)
input_year = widgets.RadioButtons(
    options=[2015, 2016], description="Year", disabled=False
)

input_area = widgets.Dropdown(
    options=all_areas,
    value=35620,
    description="Area of interest",
    layout=Layout(width="80%"),
)


input_fms = widgets.SelectMultiple(
    options=all_fms, description="Industries", layout=Layout(width="80%"), rows=10
)

input_outcomes = widgets.SelectMultiple(
    options=all_outcomes, description="Outcomes", layout=Layout(width="80%"), rows=10
)

input_population = widgets.Checkbox(
    value=False, description="Add population", disabled=False
)


def show_peers(df_county_dist, df_msa_def, area, n_peers, year):
    if area in list(df_data['AREA']):
        geo_level = 'msa'
    peers, fms = find.get_geographic_peers(
        df_data, df_county_dist, df_msa_def, area, n_peers,
        year, geo_level=geo_level
    )
    df_peers = pd.DataFrame({"Area": [str(x) for x in peers]})
    df_peers["Peer Code"] = df_peers["Area"].astype(str)
    df_peers["Peer Name"] = df_peers["Peer Code"].apply(code2name)
    df_peers["Is peer"] = 1
    return df_peers[["Peer Name", "Peer Code"]]


def show_fms_peers(
    area,
    year,
    n_peers,
    fms,
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
        only_fms = fms
    peers, fms = find.get_peers_from_input(
        df_data, area, year, n_peers, fms, outcomes)
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, area, peers, [
                   x + "-PC_EMPL" for x in only_fms], year, show=True)
    for i in fms:
        vis.duo_fm_viz(df_data, area, [area]
                       + peers, i, year, save_fig=None, show=True)
    df_peers = pd.DataFrame({"Peer Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer Code"].apply(code2name)
    return df_peers


def show_disting_peers(area, year, n_peers, n_feat, filter_pop, save_fig):
    peers, fms = find.get_distinguishing_features_peers(
        df_data, area, year, n_peers, n_feat, filter_pop=filter_pop
    )
    print(
        f"Comparison of {code2name(area)} and its peers for its {n_feat} most distinguishing traits")
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, area, peers, fms, year,
                   save_fig=f"{save_fig}_{area}_all_top.png", show=True)
    for i in fms:
        vis.duo_fm_viz(df_data, area, [area] + peers, i, year,
                       save_fig=f"{save_fig}_{area}_top_{i}.png", show=True)
    df_peers = pd.DataFrame({"Peer Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer Code"].apply(code2name)
    return df_peers


def show_top_fms_peers(area, year, n_peers, n_fms, filter_pop, save_fig):
    peers, fms = find.get_top_n_fms_peers(
        df_data, area, year, n_peers, n_fms, filter_pop=filter_pop
    )
    print(f"Comparison of {code2name(area)} and its peers for its {n_fms} most present industries")
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, area, peers, fms, year,
                   save_fig=f"{save_fig}_{area}_dist_all.png", show=True)
    for i in fms:
        vis.duo_fm_viz(df_data, area, [area] + peers, i, year,
                       save_fig=f"{save_fig}_{area}_dist_{i}.png", show=True)
    df_peers = pd.DataFrame({"Peer Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer Code"].apply(code2name)
    return df_peers


def show_coverage_peers(area, year, n_peers, coverage,  filter_pop):
    coverage = coverage / 10
    peers, fms = find.get_emp_threshold_peers(
        df_data, area, year, n_peers, coverage, filter_pop=filter_pop
    )
    pretty_prints(peers, fms)
    vis.bar_all_fm(df_data, area, peers, fms, year)
    for i in fms:
        vis.duo_fm_viz(df_data, area, [area]
                       + peers, i, year, save_fig=None, show=True)
    df_peers = pd.DataFrame({"Peer Code": [str(x) for x in peers]})
    df_peers["Peer Name"] = df_peers["Peer Code"].apply(code2name)
    return df_peers
