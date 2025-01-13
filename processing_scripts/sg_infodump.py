import pandas as pd
import sys
import numpy as np

# Specify dataset file
if len(sys.argv) > 1:
    storename = sys.argv[1]
else:
    storename = 'CollectorStore.hd5'

# Load data
store = pd.HDFStore(storename)
df = store['Collector']  # Accessing the 'Collector' data group
store.close()

# Inspect available data columns
print("Available data columns:")
print(df.columns)

# Filtering for SmartMeter and Phasor data
smartmeter_data = df[[col for col in df.columns if "SmartMeter" in col]]
phasor_data = df[[col for col in df.columns if "Phasor" in col]]

# Function to parse device data
def parse_device_data(device_data, device_type):
    parsed_data = []
    print(f"\n--- Parsing {device_type} Data ---")
    for col in device_data.columns:
        print(f"Processing Device: {col}")
        try:
            # Extract data for each device
            value_data = device_data[col].iloc[0] if len(device_data[col]) > 0 else None
            time_data = device_data[col].iloc[1] if len(device_data[col]) > 1 else None
            event_state_data = device_data[col].iloc[2] if len(device_data[col]) > 2 else None

            # Loop through the available data
            if isinstance(value_data, (list, np.ndarray)) and isinstance(time_data, (list, np.ndarray)):
                for i, (value, time) in enumerate(zip(value_data, time_data)):
                    if isinstance(value, dict):
                        # Extract relevant fields based on device type
                        if device_type == "SmartMeter":
                            va = value.get('VA', None)
                            vb = value.get('VB', None)
                            vc = value.get('VC', None)
                            spa = value.get('SPA', None)
                            spb = value.get('SPB', None)
                            spc = value.get('SPC', None)
                        elif device_type == "Phasor":
                            va = value.get('VA', (None, None))[0]
                            va_phase = value.get('VA', (None, None))[1]
                            ia = value.get('IA', (None, None))[0]
                            ia_phase = value.get('IA', (None, None))[1]
                            vb = value.get('VB', (None, None))[0]
                            vb_phase = value.get('VB', (None, None))[1]
                            ib = value.get('IB', (None, None))[0]
                            ib_phase = value.get('IB', (None, None))[1]
                            vc = value.get('VC', (None, None))[0]
                            vc_phase = value.get('VC', (None, None))[1]
                            ic = value.get('IC', (None, None))[0]
                            ic_phase = value.get('IC', (None, None))[1]

                        # Parse event_state if available
                        if isinstance(event_state_data, (list, np.ndarray)) and len(event_state_data) > i:
                            event_state = event_state_data[i]
                            if isinstance(event_state, dict):
                                event_normal = int(event_state.get('Normal', False))
                                event_fault = int(event_state.get('Fault', False))
                                event_generator_trip = int(event_state.get('GeneratorTrip', False))
                                event_load_change = int(event_state.get('LoadChange', False))
                            else:
                                event_normal = event_fault = event_generator_trip = event_load_change = None
                        else:
                            event_normal = event_fault = event_generator_trip = event_load_change = None

                        # Append parsed data
                        if device_type == "SmartMeter":
                            parsed_data.append({
                                'Timestamp': time,
                                'MeterID': value.get('IDT', None),
                                'VA': va, 'VB': vb, 'VC': vc,
                                'SPA': spa, 'SPB': spb, 'SPC': spc,
                                'Event_Normal': event_normal,
                                'Event_Fault': event_fault,
                                'Event_GeneratorTrip': event_generator_trip,
                                'Event_LoadChange': event_load_change
                            })
                        elif device_type == "Phasor":
                            parsed_data.append({
                                'Timestamp': time,
                                'PhasorID': value.get('IDT', None),
                                'VA_Magnitude': va, 'VA_Phase': va_phase,
                                'IA_Magnitude': ia, 'IA_Phase': ia_phase,
                                'VB_Magnitude': vb, 'VB_Phase': vb_phase,
                                'IB_Magnitude': ib, 'IB_Phase': ib_phase,
                                'VC_Magnitude': vc, 'VC_Phase': vc_phase,
                                'IC_Magnitude': ic, 'IC_Phase': ic_phase,
                                'Event_Normal': event_normal,
                                'Event_Fault': event_fault,
                                'Event_GeneratorTrip': event_generator_trip,
                                'Event_LoadChange': event_load_change
                            })
        except Exception as e:
            print(f"  Error processing column {col}: {e}")

    return pd.DataFrame(parsed_data)

# Parse SmartMeter and Phasor data
parsed_smartmeter_data = parse_device_data(smartmeter_data, "SmartMeter")
parsed_phasor_data = parse_device_data(phasor_data, "Phasor")

# Save parsed data to CSV
if not parsed_smartmeter_data.empty:
    parsed_smartmeter_data.to_csv('Parsed_SmartMeter_data.csv', index=False)
    print("\nParsed SmartMeter data has been exported to 'Parsed_SmartMeter_data.csv'.")
    print(parsed_smartmeter_data.head())

if not parsed_phasor_data.empty:
    parsed_phasor_data.to_csv('Parsed_Phasor_data.csv', index=False)
    print("\nParsed Phasor data has been exported to 'Parsed_Phasor_data.csv'.")
    print(parsed_phasor_data.head())
else:
    print("No valid Phasor data found.")
