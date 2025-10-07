#!/usr/bin/env python3
import pandas as pd
from pathlib import Path
import argparse

def main(data_dir):
    data_dir = Path(data_dir)
    report_files = sorted(data_dir.glob("spread_report_*.csv"))
    # Escludi file già aggregati
    report_files = [f for f in report_files if "all_pairs" not in f.name]

    if not report_files:
        print("Nessun report da processare.")
        return

    long_list = []
    print(f"Found {len(report_files)} report files. Processing...")
    for f in report_files:
        df = pd.read_csv(f)
        if df.empty:
            print(f"  SKIP: {f.name} è vuoto")
            continue
        # Determina pair dal nome file
        pair = f.stem.replace("spread_report_", "")
        df = df.rename(columns={df.columns[0]: "date"})
        df['date'] = pd.to_datetime(df['date'], utc=True)
        df['pair'] = pair
        df = df.drop_duplicates(subset=['date', 'pair'], keep='first')
        long_list.append(df[['date', 'pair', 'spread_pct']])
        print(f"  OK: {f.name} -> {len(df)} rows; index range: {df['date'].min()} to {df['date'].max()}")

    # Concatenazione LONG
    long_df = pd.concat(long_list, ignore_index=True)
    long_df = long_df.sort_values(['pair', 'date']).drop_duplicates(subset=['date', 'pair'], keep='first')
    long_out = data_dir / 'spread_report_all_pairs_long.csv'
    long_df.to_csv(long_out, index=False)
    print(f"Saved LONG consolidated -> {long_out} ({len(long_df)} rows)")

    # Costruzione WIDE
    try:
        wide_df = long_df.pivot_table(index='date', columns='pair', values='spread_pct', aggfunc='first')
    except Exception as e:
        print(f"Pivot failed: {e}. Attempting fallback per pair.")
        pieces = []
        for pair_name in long_df['pair'].unique():
            sub = long_df[long_df['pair']==pair_name].set_index('date')[['spread_pct']]
            sub = sub[~sub.index.duplicated(keep='first')]
            sub = sub.rename(columns={'spread_pct': pair_name})
            pieces.append(sub)
        wide_df = pd.concat(pieces, axis=1)
    
    wide_df = wide_df.sort_index()
    wide_out = data_dir / 'spread_report_all_pairs_wide.csv'
    wide_df.to_csv(wide_out, index=True)
    print(f"Saved WIDE consolidated -> {wide_out} (shape: {wide_df.shape})")

    # Excel opzionale
    try:
        excel_out = data_dir / 'spread_report_all_pairs.xlsx'
        with pd.ExcelWriter(excel_out) as writer:
            long_df.to_excel(writer, sheet_name='LONG', index=False)
            wide_df.to_excel(writer, sheet_name='WIDE')
        print(f"Saved Excel -> {excel_out}")
    except Exception as e:
        print(f"Could not write Excel: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Robust merge for ArbiSense spread reports')
    parser.add_argument('--data-dir', type=Path, default=Path('.') / 'data_sample', help='Directory with spread_report CSVs')
    args = parser.parse_args()
    main(args.data_dir)

