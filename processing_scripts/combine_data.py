import pandas as pd
import numpy as np

# Deduplication function
def deduplicate_dataset(df, subset=None):
    """Deduplicate a DataFrame based on a subset of columns."""
    return df.drop_duplicates(subset=subset)

# Handling missing timestamps for NS3
def handle_missing_timestamps_ns3(ns3_df, all_timestamps):
    """Fill in missing NS3 timestamps by inserting empty rows for missing values."""
    ns3_timestamps = set(ns3_df['Timestamp'].unique())
    missing_timestamps = all_timestamps - ns3_timestamps

    if missing_timestamps:
        print(f"Missing NS3 timestamps: {sorted(missing_timestamps)}")
        for ts in missing_timestamps:
            # Append rows with NaN values for missing timestamps
            ns3_df = pd.concat(
                [ns3_df, pd.DataFrame({'Timestamp': [ts], 'packets_sent': [0], 'packets_received': [0], 
                                       'packets_dropped': [0], 'packet_size_mean': [np.nan], 'packet_size_var': [np.nan]})],
                ignore_index=True
            )
    return ns3_df.sort_values('Timestamp').reset_index(drop=True)

# Combine data
def combine_data():
    chunk_size = 1000

    # Read datasets
    phasor_df = pd.read_csv("Parsed_Phasor_data.csv")
    smartmeter_df = pd.read_csv("Parsed_SmartMeter_data.csv")
    ns3_df = pd.read_csv("Aggregated_Network_Data.csv")  # Adjusted file name for simplicity
    dse_df = pd.read_csv("state_estimation_results.csv")

    # Deduplicate all datasets
    phasor_df = deduplicate_dataset(phasor_df, subset=['Timestamp', 'PhasorID'])
    smartmeter_df = deduplicate_dataset(smartmeter_df, subset=['Timestamp', 'MeterID'])
    ns3_df = deduplicate_dataset(ns3_df, subset=['Timestamp'])
    dse_df = deduplicate_dataset(dse_df, subset=['Timestamp'])

    # Collect all unique timestamps
    all_timestamps = set(phasor_df['Timestamp'].unique()) | set(smartmeter_df['Timestamp'].unique()) | \
                     set(ns3_df['Timestamp'].unique()) | set(dse_df['Timestamp'].unique())

    # Handle missing timestamps in NS3
    ns3_df = handle_missing_timestamps_ns3(ns3_df, all_timestamps)

    # Check DSE timestamp alignment
    dse_timestamps = set(dse_df['Timestamp'].unique())
    missing_dse_timestamps = all_timestamps - dse_timestamps
    if missing_dse_timestamps:
        print(f"Missing DSE timestamps: {sorted(missing_dse_timestamps)}")
        for ts in missing_dse_timestamps:
            dse_df = pd.concat(
                [dse_df, pd.DataFrame({'Timestamp': [ts], 'Node_0_Magnitude': [np.nan], 'Node_0_Phase': [np.nan]})],
                ignore_index=True
            )

    # Pivot phasor data
    phasor_df['PhasorID'] = phasor_df['PhasorID'].astype(str)
    phasor_wide = phasor_df.pivot_table(
        index='Timestamp',
        columns='PhasorID',
        values=['VA_Magnitude', 'VA_Phase', 'IA_Magnitude', 'IA_Phase',
                'Event_Normal', 'Event_Fault', 'Event_GeneratorTrip', 'Event_LoadChange']
    )
    phasor_wide.columns = ['{}_{}'.format(var, phasor) for var, phasor in phasor_wide.columns]
    phasor_wide = phasor_wide.reset_index()

    # Pivot smartmeter data
    smartmeter_df['MeterID'] = smartmeter_df['MeterID'].astype(str)
    smartmeter_wide = smartmeter_df.pivot_table(
        index='Timestamp',
        columns='MeterID',
        values=['VA', 'SPA', 'Event_Normal', 'Event_Fault', 'Event_GeneratorTrip', 'Event_LoadChange']
    )
    smartmeter_wide.columns = ['{}_{}'.format(var, meter) for var, meter in smartmeter_wide.columns]
    smartmeter_wide = smartmeter_wide.reset_index()

    # Merge all datasets
    combined_df = pd.DataFrame({'Timestamp': sorted(all_timestamps)})
    combined_df = combined_df.merge(phasor_wide, on='Timestamp', how='left')
    combined_df = combined_df.merge(smartmeter_wide, on='Timestamp', how='left')
    combined_df = combined_df.merge(ns3_df, on='Timestamp', how='left')
    combined_df = combined_df.merge(dse_df, on='Timestamp', how='left')

    # Fill missing values (e.g., forward-fill for continuity where appropriate)
    combined_df.fillna(method='ffill', inplace=True)

    # Save combined dataset
    combined_df.to_csv("Combined_Dataset.csv", index=False)
    print("Combined dataset saved as 'Combined_Dataset.csv'")

# Run combination
combine_data()
