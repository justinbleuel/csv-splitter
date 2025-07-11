#!/usr/bin/env python3
"""
Test script to create a large CSV file for testing the timeout fix
"""
import csv
import random
import string
import sys

def generate_random_data(num_rows, num_cols=10):
    """Generate a large CSV file with random data"""
    
    # Generate headers
    headers = [f"Column_{i+1}" for i in range(num_cols)]
    
    filename = f"test_large_{num_rows}_rows.csv"
    
    print(f"Generating CSV file with {num_rows:,} rows and {num_cols} columns...")
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        
        # Write headers
        writer.writerow(headers)
        
        # Write data rows
        for i in range(num_rows):
            row = []
            for j in range(num_cols):
                # Mix different types of data
                if j % 3 == 0:
                    # Random string
                    row.append(''.join(random.choices(string.ascii_letters, k=10)))
                elif j % 3 == 1:
                    # Random number
                    row.append(random.randint(1, 1000000))
                else:
                    # Random float
                    row.append(round(random.uniform(0, 1000), 2))
            
            writer.writerow(row)
            
            # Progress indicator
            if (i + 1) % 100000 == 0:
                print(f"  Generated {i+1:,} rows...")
    
    # Get file size
    import os
    file_size = os.path.getsize(filename) / (1024 * 1024)  # Size in MB
    
    print(f"\nGenerated: {filename}")
    print(f"File size: {file_size:.2f} MB")
    print(f"Total rows: {num_rows:,}")
    
    return filename

if __name__ == "__main__":
    # Default: generate a file that's likely ~108MB
    num_rows = 2_000_000  # 2 million rows
    
    if len(sys.argv) > 1:
        try:
            num_rows = int(sys.argv[1])
        except ValueError:
            print("Usage: python test_large_file.py [number_of_rows]")
            sys.exit(1)
    
    generate_random_data(num_rows)