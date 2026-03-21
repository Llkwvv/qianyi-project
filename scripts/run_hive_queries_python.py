#!/usr/bin/env python3
"""
Python script to execute Hive SQL queries and save results to a file
in the format matching the old_summary table structure.
"""

import subprocess
import pandas as pd
from datetime import datetime
import os

def execute_hive_query(query):
    """Execute a Hive query on a remote server via SSH and return results as DataFrame"""
    try:
        # Get remote server configuration from environment variables
        remote_host = os.getenv('HIVE_REMOTE_HOST', '172.20.10.6')
        remote_user = os.getenv('HIVE_REMOTE_USER', 'atguigu')
        hive_bin_path = os.getenv('HIVE_BIN_PATH', '/opt/module/hive/bin/hive')

        # Construct the SSH command with proper escaping for Hive query
        # We'll use the -S flag to pass the query directly to avoid escaping issues
        ssh_command = [
            'ssh',
            '-o',
            'StrictHostKeyChecking=no',
            f'{remote_user}@{remote_host}',
            f'{hive_bin_path} -e "{query}"'
        ]

        # Execute the SSH command and capture output
        result = subprocess.run(
            ssh_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True
        )

        # Parse the output (assuming tab-separated values)
        lines = result.stdout.strip().split('\n')
        if len(lines) <= 1:
            return pd.DataFrame()

        # Split header and data
        header = lines[0].split('\t')
        data_lines = [line.split('\t') for line in lines[1:] if line.strip()]

        # Create DataFrame
        df = pd.DataFrame(data_lines, columns=header)
        return df
    except subprocess.CalledProcessError as e:
        print(f"Error executing Hive query via SSH: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"Unexpected error: {e}")
        return pd.DataFrame()

def main():
    # Read queries from SQL file
    queries = []
    try:
        with open('/home/lkw/qianyi-project/scripts/metrics_queries.sql', 'r') as f:
            content = f.read()

        # Extract SELECT statements (skip comments and empty lines)
        lines = content.strip().split('\n')
        current_query = ""

        for line in lines:
            line = line.strip()
            # Skip comments and empty lines
            if line.startswith('--') or not line:
                continue

            # Add line to current query
            current_query += line + " "

            # If line ends with semicolon, it's a complete query
            if line.endswith(';'):
                # Clean up the query
                query = current_query.strip()
                if query:
                    queries.append(query)
                current_query = ""

    except Exception as e:
        print(f"Error reading queries from SQL file: {e}")
        # Fallback to hardcoded queries if file reading fails
        # Removed hardcoded queries as requested
        queries = []

    # Create an empty DataFrame to store all results
    all_results = []

    # Execute each query
    for i, query in enumerate(queries):
        print(f"Executing query {i+1}...")
        df = execute_hive_query(query)

        if not df.empty:
            # Convert the dataframe to the expected format
            # Assuming all queries return the same structure now
            result_row = {
                'table_name': df['table_name'].iloc[0],
                'check_type': 'row_count',
                'metric_name': 'row_count',
                'metric_expr': 'count(1)',
                'value': df['row_count'].iloc[0],
                'partition_spec': df['partition_spec'].iloc[0],
                'computed_at': df['computed_at'].iloc[0],
                'data_dt': df['data_dt'].iloc[0]
            }

            all_results.append(result_row)
        else:
            print(f"Query {i+1} returned no results")

    # Create final DataFrame
    if all_results:
        final_df = pd.DataFrame(all_results)

        # Save to CSV file in the same format as expected by create_old_summary.sql
        output_file = '/home/lkw/qianyi-project/scripts/old_summary_data.csv'
        final_df.to_csv(output_file, index=False, sep='\t')
        print(f"Results saved to {output_file}")

        # Print a sample of the results
        print("\nSample results:")
        print(final_df.head())

        # Optional: Show the structure that matches the table schema
        print("\nFinal data structure (matching old_summary table):")
        print("table_name, check_type, metric_name, metric_expr, value, partition_spec, computed_at, data_dt")
    else:
        print("No results to save")

if __name__ == "__main__":
    main()