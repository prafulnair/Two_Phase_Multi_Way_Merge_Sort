"""
This is the first Lab Project for COMP 6521
Coded by Praful Peethambaran Nair (40226483), Aashray Munjal (), Tanay Ashish Srivastava (40234148)

"""

"""
Database Schema Details
Employee ID  = 8 chars
Employee Name = 25 chars
LastUpdate = 10 chars // does not exist, so ignore this
Gender = 1 char (0 or 1)
Department = 3 chars
Social Insurance Number = 9 chars
Address = 43 chars. 
"""

"""
We can store each record as a tuple of two data. The first part would contain 123456782024, which is the employee id
and the second part would contain the rest of the data, 
12223333 de Maisonneuve West, Montreal, QC, H3G 1M8, Canada
"""


import sys
from heapq import merge
from tqdm import tqdm

def generate_runs(main_list, run_size):
    runs = [] 
    total_records = len(main_list)
    num_runs = total_records // run_size
    remainder = total_records % run_size

    for i in range(num_runs):
        run = {} # Creating dictionary to store emp ID as key and rest of the string as value
        for j in range(run_size):
            record = main_list[i*run_size+j] # extracting 10 / 20 records for the runs
            key = int(record[:8].strip()) # extracting the employee ID
            value = record[8:]
            run[key] = value
        sorted_run = dict(sorted(run.items()))
        runs.append(sorted_run)

    if remainder > 0:
        run = {}
        for i in range(remainder):
            record = main_list[num_runs * run_size + i]
            key = int(record[:8].strip())
            value = record[8:]
            run[key] = value
        sorted_run = dict(sorted(run.items()))
        runs.append(sorted_run)

    return runs 

    # Now you have variables run_1, run_2, ..., run_n, each containing an empty list

def read_data_main2(filename1, filename2):
    # Creating the main buffer. main_list will contain data from both the files..
    # This is a temporary storage before we split the data into multiple runs based on array size. 
    main_list = []

    # For reading total number of rows in the file1
    with open(filename1,'r') as file:
        total_records1 = len(file.readlines())

    # Reading the data from the file1 to main_list 
    with open(filename1, "r") as file:
        rows1 = 0
        for line in tqdm(file, total=total_records1, desc='Reading File 1'):
            main_list.append(line)  

    # For reading total number of rows in the file2
    with open(filename2,'r') as file:
        total_records2 = len(file.readlines())

    # Reading the data from the file2 to main_list
    with open(filename2, "r") as file:
        rows2 = 0
        for line in tqdm(file, total=total_records2, desc='Reading File 2'):
            main_list.append(line)  
            rows2 += 1

    return main_list, total_records1+total_records2

def tpmms(sorted_runs):
    num_runs = len(sorted_runs)
    
    # First pass: Merge runs into groups that fit into memory
    group_size = 10  # Adjust group size based on available memory
    groups = [sorted_runs[i:i+group_size] for i in range(0, num_runs, group_size)]
    
    # Second pass: Merge runs within each group
    merged_groups = []
    for group in groups:
        merged_run = group[0] if len(group) == 1 else dict(merge(*[sorted(run.items()) for run in group]))
        merged_groups.append(merged_run)
    
    # Final pass: Merge merged runs from each group
    merged_runs = merged_groups[0] if len(merged_groups) == 1 else dict(merge(*[sorted(group.items()) for group in merged_groups]))
    
    return merged_runs

def write_to_file(merged_runs, output_file, disk_ios, runs):
    # Count unique records
    unique_records = len(merged_runs)
    
    # Calculate number of Disk I/Os
    total_disk_ios = disk_ios * 10  # Each Disk I/O represents the transfer of 10 records
    
    # Create a dictionary to store the count of each record
    record_counts = {}
    for key, value in merged_runs.items():
        record_counts[key] = 1  # Initialize count to 1 for the first occurrence of each record

    # Count occurrences of each record in the runs
    for run in runs:
        for key in run.keys():
            if key in record_counts:
                record_counts[key] += 1

    # Sort records by their IDs
    sorted_records = sorted(merged_runs.items(), key=lambda x: x[0])

    # Write to output file
    with open(output_file, 'w') as file:
        file.write(f"Record Count = {unique_records}\n")
        file.write(f"# Disk I/Os = {total_disk_ios}\n")
        for key, value in record_counts.items():
            file.write(f"{key}:{value}\n")
        # Write sorted merged runs
        for key, value in sorted_records:
            file.write(f"Employee ID: {key}, Record: {value}\n")



if __name__ == "__main__":

    if len(sys.argv)!= 3:
        print("Please specify 2 filenames")
        print("Sample usage: python3 main.py filename1 filename2")
        sys.exit(1)
    
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]

    main_list, rows = read_data_main2(filename1, filename2)



    # uncomment the following for loop if you want to print the loaded data
    # for records in main_list:
    #     print(records)
    # print("TOTAL Records read and stored in main buffer: ",rows)

    runs = generate_runs(main_list, run_size=10)

    merged_runs = tpmms(runs)

    # for run in runs:
    #     for keys in run.keys():
    #         print(keys)



        # Print merged runs
    for key, value in merged_runs.items():
        print(f"Employee ID: {key}, Record: {value}")

    print(f"Number of merged runs: {len(merged_runs)}")
    write_to_file(merged_runs, 'output.txt', len(runs), runs)
    print(len(runs))


