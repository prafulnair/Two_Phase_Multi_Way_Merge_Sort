"""
This is the first Lab Project for COMP 6521
Coded by Praful Peethambaran Nair (40226483)
"""

"""
Database Schema Details
Employee ID  = 8 chars
Employee Name = 25 chars
LastUpdate = 10 chars
Gender = 1 char (0 or 1)
Department = 3 chars
Social Insurance Number = 9 chars
Address = 43 chars. 
"""

"""
We can store each record as a tuple of two data. The first part would contain 123456782024-01-15John, 
and the second part would contain the rest of the data, 
12223333333331455 de Maisonneuve West, Montreal, QC, H3G 1M8, Canada

This is because 
"""


import sys
from tqdm import tqdm

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
    print("TOTAL Records read and stored in main buffer: ",rows)

    create_runs(main_list)
