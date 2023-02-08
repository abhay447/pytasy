from etl.datawriter import create_match_file_list, write_match_file
match_file_list = create_match_file_list(raw_data_prefix = '/home/abhay/work/dream11/raw_historical_data')

for i in range(len(match_file_list)):
    full_match_file = match_file_list[i]
    print("reading file %s, %i out of %i"%(full_match_file, i, len(match_file_list)))
    write_match_file(full_match_file, '/home/abhay/work/dream11/processed_output')