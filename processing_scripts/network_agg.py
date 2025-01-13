import pandas as pd

# Function to process and aggregate network data
def process_and_aggregate_network_data(file_paths, output_file_granular, output_file_summary, chunk_size=10000):
    aggregated_data = []

    for file_path in file_paths:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            # Convert to milliseconds and round
            chunk['Timestamp'] = (chunk['Timestamp'] * 1000).round().astype(int)
            
            # Group by 1ms window and NodeID, aggregating data within each window
            agg_chunk = chunk.groupby(['Timestamp', 'NodeID']).agg(
                packets_sent=('EventType', lambda x: (x == 'sent').sum()),
                packets_received=('EventType', lambda x: (x == 'received').sum()),
                packets_dropped=('EventType', lambda x: (x == 'dropped').sum()),
                packet_size_mean=('PacketSize', 'mean'),
                packet_size_var=('PacketSize', 'var')
            ).reset_index()

            aggregated_data.append(agg_chunk)

    # Concatenate all aggregated data
    aggregated_df = pd.concat(aggregated_data, ignore_index=True)

    # Drop any rows with NaN in Timestamp
    aggregated_df = aggregated_df.dropna(subset=['Timestamp'])

    # Ensure the Timestamp column is of integer type
    aggregated_df['Timestamp'] = aggregated_df['Timestamp'].astype(int)

    # Fill in missing timestamps by creating a DataFrame with all the timestamps in the range
    if not aggregated_df.empty:
        min_timestamp = int(aggregated_df['Timestamp'].min())
        max_timestamp = int(aggregated_df['Timestamp'].max())

        # Create a DataFrame to ensure all millisecond timestamps are present
        all_timestamps = pd.DataFrame({'Timestamp': range(min_timestamp, max_timestamp + 1)})
        aggregated_df = pd.merge(all_timestamps, aggregated_df, on='Timestamp', how='left')
        aggregated_df.fillna(method='ffill', inplace=True)

    # Save the granular aggregated data to a CSV
    aggregated_df.to_csv(output_file_granular, index=False)

    # Create a summary by grouping only by Timestamp (ignoring NodeID)
    summary_df = aggregated_df.groupby('Timestamp').agg(
        packets_sent=('packets_sent', 'sum'),
        packets_received=('packets_received', 'sum'),
        packets_dropped=('packets_dropped', 'sum'),
        packet_size_mean=('packet_size_mean', 'mean'),
        packet_size_var=('packet_size_var', 'mean')
    ).reset_index()

    # Save the summarized data to a CSV
    summary_df.to_csv(output_file_summary, index=False)

# Aggregate network data and save to CSVs
process_and_aggregate_network_data(
    ["Parsed_Network_Netsim.csv", "Parsed_Network_Sixlowpan.csv"],
    "Aggregated_Network_Data_Granular.csv",
    "Aggregated_Network_Data_Summary.csv"
)
