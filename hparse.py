import re
import csv

def parse_file_to_csv(infile, outfile):
    # Node line: TIMESTAMP HOSTNAME ARCH NCPU ...  (no leading whitespace after timestamp)
    node_pattern = re.compile(r'^(\d+)\s+(\S+)\s+\S+\s+(\d+)')
    # Queue line: TIMESTAMP <spaces>QUEUENAME ... a/b/c
    queue_pattern = re.compile(r'^(\d+)\s+ +(\S+)\s+\S+\s+(\d+)/(\d+)/(\d+)')
    
    rows = []
    curr_timestamp = None
    curr_hostname = None
    curr_ncpu = None
    total_used = 0
    seen_node = False

    with open(infile) as f:
        for line in f:
            line = line.rstrip()
            # Try node match
            node_match = node_pattern.match(line)
            queue_match = queue_pattern.match(line)

            if node_match and (not queue_match):  # It's a real node line, not a queue!
                # If we have a previous node's info, output its result before starting new node
                if seen_node:
                    rows.append([curr_timestamp, curr_hostname, curr_ncpu, total_used])
                # Start new node
                curr_timestamp = node_match.group(1)
                curr_hostname = node_match.group(2)
                curr_ncpu = int(node_match.group(3))
                total_used = 0
                seen_node = True
            elif queue_match:
                # This is a queue for the current node: sum the middle number in a/b/c (b = group(4))
                # Only add if the timestamp matches the current node (should always be true for this format)
                if queue_match.group(1) == curr_timestamp:
                    total_used += int(queue_match.group(4))
        # After loop, output last node
        if seen_node:
            rows.append([curr_timestamp, curr_hostname, curr_ncpu, total_used])

    # Write to CSV
    with open(outfile, "w", newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['timestamp', 'hostname', 'ncpu', 'total_used'])
        writer.writerows(rows)


if __name__ == "__main__":
    parse_file_to_csv("/project/scv/dugan/sge/data/2508.h", "outputtest.csv")
