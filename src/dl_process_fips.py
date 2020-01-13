import pandas as pd


df_titles = pd.read_csv('data/external/geocodes_names.csv')
df_titles = df_titles[df_titles['']]
df_titles = df_titles[['AREA', 'AREA_NAME']].drop_duplicates()
df_titles.set_index('AREA', inplace=True)
print(df_titles)
# df['AREA'] = df['STATE'].astype(str).apply(str.zfill, args=(2,)) + \
#     df['COUNTY'].astype(str).apply(str.zfill, args=(3,))

# df.to_csv('data/external/geocodes.csv')
# df_county_metrics = pd.read_csv('data/processed/county_metrics_outcomes.csv')
#
# df_county_metrics.set_index('AREA', inplace=True)
# df_county_metrics = df_county_metrics.merge(df_titles, left_index=True,
#                                             right_index=True)
# print(df_county_metrics.head())
# df_county_metrics.to_csv('data/external/county_metrics_outcomes.csv')
