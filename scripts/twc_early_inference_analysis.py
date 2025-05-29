import pandas as pd
col = ['sip', 'sport', 'dip', 'dport', 'proto', 'first_timestamp', 'app',
    #    'sni',   'dd', 'dn', 'dns', 'ds', 'app',
    'application']
merge_key = ['sip', 'sport', 'dip', 'dport', 'proto', 'first_timestamp']


def compare_predictions(merged_df, pkt_size):
        total_rows = len(merged_df)
        col = f'app_{pkt_size}'
        matching_predictions = (merged_df['app_5000pkt'] == merged_df[col]).sum()
        different_predictions = total_rows - matching_predictions

        print(f"\nPrediction Comparison: 5000pkt vs {pkt_size}")
        print("=" * 50)
        print(f"Total flows analyzed: {total_rows}")
        print(f"Matching predictions: {matching_predictions}")
        print(f"Unmatched predictions: {different_predictions}")
        print(f"Percentage matching: {(matching_predictions/total_rows)*100:.2f}%")
        print(f"Percentage unmatched: {(different_predictions/total_rows)*100:.2f}%")

        # Show the flows where predictions differ
        different_flows = merged_df[merged_df['app_5000pkt'] != merged_df[col]]
        print("\nFlows with different predictions:")
        print(f"5000pkt prediction | {pkt_size} prediction | Count")
        print("-" * 50)
        
        prediction_differences = different_flows.groupby(['app_5000pkt', col]).size()
        for (pred_5000, pred_pkt_size), count in prediction_differences.items():
            print(f"{pred_5000:<18} | {pred_pkt_size:<18} | {count}")
            
        # Add comparison column showing if predictions match
        merged_df['comparison'] = 'false    '
        merged_df.loc[merged_df['app_5000pkt'] == merged_df[col], 'comparison'] = 'true'
        
        merged_df.to_csv(f"/home/mpaul/projects/mpaul/mai/mai-streaming/results/merged_df_{pkt_size}.csv", index=False)

def main():
    df_5000 = pd.read_csv("/home/mpaul/projects/mpaul/mai/mai-streaming/results/twc_test_5000pkt.csv", usecols=col)
    df_5000.rename(columns={'application': 'application_5000pkt', 'app': 'app_5000pkt'}, inplace=True)
    
    df_500 = pd.read_csv("/home/mpaul/projects/mpaul/mai/mai-streaming/results/twc_test_500pkt.csv", usecols=col)
    df_500.rename(columns={'application': 'application_500pkt', 'app': 'app_500pkt'}, inplace=True)
    
    df_1000 = pd.read_csv("/home/mpaul/projects/mpaul/mai/mai-streaming/results/twc_test_1000pkt.csv", usecols=col)
    df_1000.rename(columns={'application': 'application_1000pkt', 'app': 'app_1000pkt'}, inplace=True)
    
    df_2000 = pd.read_csv("/home/mpaul/projects/mpaul/mai/mai-streaming/results/twc_test_2000pkt.csv", usecols=col)
    df_2000.rename(columns={'application': 'application_2000pkt', 'app': 'app_2000pkt'}, inplace=True)

    df_30 = pd.read_csv("/home/mpaul/projects/mpaul/mai/mai-streaming/results/twc_test_30pkt.csv", usecols=col)
    df_30.rename(columns={'application': 'application_30pkt', 'app': 'app_30pkt'}, inplace=True)

    # # Merge all three dataframes on the merge key
    merged_df_30pkt = df_5000.merge(df_30, on=merge_key, how='outer')
    merged_df_500pkt = df_5000.merge(df_500, on=merge_key, how='outer')
    merged_df_2000pkt = df_5000.merge(df_2000, on=merge_key, how='outer')
    merged_df_1000pkt = df_5000.merge(df_1000, on=merge_key, how='outer')
    
    compare_predictions(merged_df_30pkt, "30pkt")
    compare_predictions(merged_df_500pkt, "500pkt")
    compare_predictions(merged_df_2000pkt, "2000pkt")
    compare_predictions(merged_df_1000pkt, "1000pkt")
    
    print("\nSummary of all Comparisons (5000pkt vs other packet sizes)")
    print("=" * 50)
    # Calculate overall summary statistics
    print("\nPacket Size | Total Flows | Matching | Match % | Unmatched | Unmatch %")
    print("-" * 65)
    
    for pkt_size in ["30pkt", "500pkt", "1000pkt", "2000pkt"]:
        df = pd.read_csv(f"/home/mpaul/projects/mpaul/mai/mai-streaming/results/merged_df_{pkt_size}.csv")
        total = len(df)
        matches = (df['comparison'] == 'true').sum()
        match_pct = (matches/total) * 100
        unmatched = (df['comparison'] == 'false    ').sum()
        unmatch_pct = (unmatched/total) * 100
        
        print(f"{pkt_size:10} | {total:11} | {matches:8} | {match_pct:7.2f}% | {unmatched:9} | {unmatch_pct:8.2f}%")
        
if __name__ == "__main__":
    main()