import re
import csv

def parse_ns3_trace(trace_file_path, output_csv_path):
    # Regex patterns for different packet events in the NS3 trace file
    packet_sent_pattern = re.compile(r'^\+\s+(\d+\.\d+)\s+/NodeList/(\d+)/.*Enqueue')
    packet_recv_pattern = re.compile(r'^\-\s+(\d+\.\d+)\s+/NodeList/(\d+)/.*Dequeue')
    packet_drop_pattern = re.compile(r'^d\s+(\d+\.\d+)\s+/NodeList/(\d+)/.*PhyRxDrop')
    
    # Metadata extraction (e.g., packet size, protocol, sequence numbers, etc.)
    metadata_pattern = re.compile(r'.*Payload Length (\d+).*')

    parsed_packets = []

    with open(trace_file_path, 'r') as trace_file:
        for line_number, line in enumerate(trace_file, start=1):
            sent_match = packet_sent_pattern.search(line)
            recv_match = packet_recv_pattern.search(line)
            drop_match = packet_drop_pattern.search(line)

            packet_metadata = None
            if "Payload Length" in line:
                metadata_match = metadata_pattern.search(line)
                if metadata_match:
                    packet_metadata = int(metadata_match.group(1))

            if sent_match:
                timestamp = float(sent_match.group(1))
                node_id = int(sent_match.group(2))
                parsed_packets.append({
                    'Timestamp': timestamp,
                    'NodeID': node_id,
                    'EventType': 'sent',
                    'PacketSize': packet_metadata
                })
            elif recv_match:
                timestamp = float(recv_match.group(1))
                node_id = int(recv_match.group(2))
                parsed_packets.append({
                    'Timestamp': timestamp,
                    'NodeID': node_id,
                    'EventType': 'received',
                    'PacketSize': packet_metadata
                })
            elif drop_match:
                timestamp = float(drop_match.group(1))
                node_id = int(drop_match.group(2))
                parsed_packets.append({
                    'Timestamp': timestamp,
                    'NodeID': node_id,
                    'EventType': 'dropped',
                    'PacketSize': packet_metadata
                })

    # Writing parsed packet data to a CSV file
    csv_columns = ['Timestamp', 'NodeID', 'EventType', 'PacketSize']
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        writer.writerows(parsed_packets)

    print(f"Parsed network trace data has been exported to '{output_csv_path}'.")

if __name__ == "__main__":
    trace_files = [
        {'input': '../traceNS3Netsim.tr', 'output': 'Parsed_Network_Netsim.csv'},
        {'input': '../traceNS3sixlowpan.tr', 'output': 'Parsed_Network_Sixlowpan.csv'}
    ]
    for trace in trace_files:
        print(f"\nAnalyzing trace file: {trace['input']}")
        parse_ns3_trace(trace['input'], trace['output'])
