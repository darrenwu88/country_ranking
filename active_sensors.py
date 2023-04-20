import os
import pandas as pd
folder_path = r'./Maldives'


final_df = pd.read_csv('active_sensors_bhutan_template.csv')

final_df['active_sensors'] = [[] for _ in range(len(final_df))]
final_df['deployed_sensor_count'] = 0
final_df['active_sensor_ratio'] = 0.0
final_df['active_hours'] = 0
final_df['possible_active_hours'] = 0
final_df['active_hour_ratio'] = 0.0

months_hours = {1: 744,
                2: 672,
                3: 744,
                4: 720,
                5: 744,
                6: 720,
                7: 744,
                8: 744,
                9: 720,
                10: 744,
                11: 720,
                12: 744}

#only need lvl 1 hourly
keyword = 'Level1_hourly'
def iterate_files(path):
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            if keyword in filename:
                yield os.path.join(dirpath, filename)
    

for file_path in iterate_files(folder_path):
    df = pd.read_csv(file_path)
    #datetime conversion
    df['Timestamp (UTC)'] = pd.to_datetime(df['Timestamp (UTC)'], format = '%m/%d/%Y %H:%M')
    df['Timestamp_strf'] = df['Timestamp (UTC)'].dt.strftime('%m/%Y')

    #75% threshold -- compare against months_hours dict
    #grouped_df = df.groupby(['serial', df['timestamp'].dt.year, df['timestamp'].dt.month])
    no_duplicates_df = df.drop_duplicates(subset = 'Timestamp_strf', keep = 'first')

    for index, row in no_duplicates_df.iterrows():
        final_df.loc[final_df['Time'] == row['Timestamp_strf'], 'active_sensors'].apply(lambda x: x.append(row['Serial Number']))
    
    #start groupby function for seeing # of rows/hours collected by sensors
    monthly_data_df = df.groupby(df['Timestamp_strf'])

    for key, group in monthly_data_df:
        num_hours = len(group)
        final_df.loc[final_df['Time'] == key, 'active_hours'] += num_hours

#can only go up 
existing_sensors = set()

for index, row in final_df.iterrows():
    for serial_number in row['active_sensors']:
        if serial_number not in existing_sensors:
            existing_sensors.add(serial_number)
            
    final_df.loc[index, 'deployed_sensor_count'] = len(existing_sensors)

    if len(existing_sensors) != 0:
        final_df.loc[index, 'active_sensor_ratio'] = f"{round((len(row['active_sensors']) / len(existing_sensors)), 2):.0%}"

    #get total possible number of sensor uptime hours for active_hour_ratio
    month = int(row['Time'].split('/')[0])
    
    tot_possible_hours = months_hours[month] * len(existing_sensors)
    final_df.loc[index, 'possible_active_hours'] = tot_possible_hours

    if tot_possible_hours != 0:
        final_df.loc[index, 'active_hour_ratio'] = f"{round((row['active_hours'] / tot_possible_hours), 2):.0%}"
 
final_df = final_df.reindex(columns=['Time', 'deployed_sensor_count', 'active_sensor_ratio', 'active_hours', 'possible_active_hours', 'active_hour_ratio',  'active_sensors'])

final_df.to_csv(r'./maldives_analysis/active_sensors_maldives.csv', index = False)






            
