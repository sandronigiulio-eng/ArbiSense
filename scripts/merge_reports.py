import os
import pandas as pd
import logging

logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

INPUT_DIR = 'data_sample'
OUTPUT_DIR = 'data_sample'

report_files = [
    'spread_report_SWDA_L_EUNL_DE.csv',
    'spread_report_CSP1_L_IUSA_DE.csv',
    'spread_report_SWDA_L_IWRD_DE.csv',
    'spread_report_VWRL_L_VEVE_AS.csv'
]
pair_names = [
    'SWDA_L_EUNL_DE',
    'CSP1_L_IUSA_DE',
    'SWDA_L_IWRD_DE',
    'VWRL_L_VEVE_AS'
]

dfs_wide = []
dfs_long = []

for file, name in zip(report_files, pair_names):
    path = os.path.join(INPUT_DIR, file)
    df = pd.read_csv(path, index_col=0, parse_dates=True)

    # Wide format: solo spread_pct
    if 'spread_pct' in df.columns:
        df_wide = df[['spread_pct']].rename(columns={'spread_pct': name})
        dfs_wide.append(df_wide)
    
    # Long format: aggiungi colonna pair senza rimuovere altre colonne
    df_long = df.copy()
    if 'pair' not in df_long.columns:
        df_long.insert(1, 'pair', name)
    dfs_long.append(df_long)

# Merge wide
df_wide_merged = pd.concat(dfs_wide, axis=1)
wide_path = os.path.join(OUTPUT_DIR, 'spread_report_all_pairs_wide.csv')
df_wide_merged.to_csv(wide_path, float_format='%.3f')
logging.info(f"Saved wide report to {wide_path}")

# Merge long
df_long_merged = pd.concat(dfs_long, axis=0)
long_path = os.path.join(OUTPUT_DIR, 'spread_report_all_pairs_long.csv')
df_long_merged.to_csv(long_path, float_format='%.3f')
logging.info(f"Saved long report to {long_path}")

