import pandas as pd
import numpy as np
import csv
import os
import sys

# Function to read device configuration
DEVS_RPATH_FILE = 'IEEE33/IEEE33_Devices_RIDE.csv'

def readDevices(devsfile):
    devParams = {}
    current_directory = os.path.dirname(os.path.realpath(__file__))
    pathToFile = os.path.abspath(
        os.path.join(current_directory, devsfile)
    )
    if not os.path.isfile(pathToFile):
        print('File Actives does not exist: ' + pathToFile)
        sys.exit()
    else:
        with open(pathToFile, 'r') as csvFile:
            csvobj = csv.reader(csvFile)
            next(csvobj)
            for rows in csvobj:
                if(len(rows) == 11):
                    instance = rows[0] + "_" +  rows[1] + "-" + rows[2] \
                                + '.' + rows[3] + '.' + rows[4]
                    devParams[instance] = {}
                    devParams[instance]['device']      = rows[0]
                    devParams[instance]['src']         = rows[1]
                    devParams[instance]['dst']         = rows[2]
                    devParams[instance]['cidx']        = rows[3]
                    devParams[instance]['didx']        = rows[4]
                    devParams[instance]['period']      = int(rows[5])
                    devParams[instance]['error']       = rows[6]
                    devParams[instance]['cktElement']  = rows[7]
                    devParams[instance]['cktTerminal'] = rows[8]
                    devParams[instance]['cktPhase']    = rows[9]
                    devParams[instance]['cktProperty'] = rows[10]
    return devParams

# Load device configuration to get timestep size
devParams = readDevices(DEVS_RPATH_FILE)

# Extract timestep size (assuming same period for smart meters and phasors)
timestep_size = devParams[next(iter(devParams))]['period']

# Read the HDF5 file
store = pd.HDFStore('CollectorStore.hd5')
df = store['Collector']
store.close()

# Extract the data for the Estimator entity
column_name = 'Estimator-0.DSESim_1'
col_data = df[column_name]

# Extract 't' and 'v'
time_steps = col_data['t']
v_data = col_data['v']

# Number of nodes (assuming constant)
num_nodes = len(v_data[0])
num_time_steps = len(time_steps)

# Initialize a DataFrame with the correct timestep intervals
time_index = pd.Series(np.arange(time_steps[0], time_steps[-1] + timestep_size, timestep_size))
voltage_df = pd.DataFrame(index=range(len(time_index)), columns=['Timestamp'] + [f'Node_{i}_Real' for i in range(num_nodes)] + [f'Node_{i}_Imag' for i in range(num_nodes)])

# Fill in the Timestamp column
voltage_df['Timestamp'] = time_index

# Fill in the voltage data by repeating the measurement until it changes
for idx, time in enumerate(time_steps):
    v_list = v_data[idx]  # List of tuples
    # Assign to DataFrame
    for node_idx, (real_part, imag_part) in enumerate(v_list):
        voltage_df.at[idx, f'Node_{node_idx}_Real'] = real_part
        voltage_df.at[idx, f'Node_{node_idx}_Imag'] = imag_part

# Forward fill missing values to match the timestep intervals
voltage_df.fillna(method='ffill', inplace=True)

# Ensure no NaN values in Timestamp column before converting to integers
voltage_df['Timestamp'] = voltage_df['Timestamp'].fillna(method='ffill').astype(int)

# Save to CSV
voltage_df.to_csv('state_estimation_results.csv', index=False)
