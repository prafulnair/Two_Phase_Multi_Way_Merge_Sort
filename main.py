def read_data_main(filename1, filename2):

    # Creating the main buffer. main_list will contain data from both the files..
    # This is a temporary storage before we split the data into multiple runs based on array size. 
    main_list = []

    # For reading total number of rows in the file1
    with open(filename1,'r') as file:
        total_records = len(file.readlines())
    
    # Reading the data from the file1 to main_list 
    rows1 = total_records
    with open(filename1, "r") as file:
        r1 = 0
        for i in range(rows1):
            record = str(file.readline())
            main_list.append(record)

    with open(filename2,'r') as file:
        total_records = len(file.readlines())
    rows2 = total_records

    # Reading the data from the file2 to main_list
    with open(filename2, "r") as file:
        for i in range(rows2):
            record = str(file.readline())
            main_list.append(record)
    
    return main_list, rows1+rows2




if __name__ == "__main__":

    if len(sys.argv)!= 3:
        print("Please specify 2 filenames")
        print("Sample usage: python3 main.py filename1 filename2")
        sys.exit(1)
    
    filename1 = sys.argv[1]
    filename2 = sys.argv[2]

    main_list, rows = read_data_main2(filename1, filename2)

    for records in main_list:
        print(records)
    print("TOTAL Records read and stored in main buffer: ",rows)
