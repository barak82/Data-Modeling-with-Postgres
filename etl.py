import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import numpy as np


def process_song_file(cur, filepath):
    """
    process the song file from folder 
    add the song data to db
    """
    # open song file
    song_df = pd.read_json(filepath,typ='series')
    song_data = song_df[['song_id','title','artist_id','year','duration']]
    # insert song record
   
    print (type(song_data[0]))
    cur.execute(song_table_insert, tuple(song_data))
    artist_data = song_df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']]
    cur.execute(artist_table_insert, tuple(artist_data))
    

def process_log_file(cur, filepath):
    """
    - process the log files in folders, extract the data 
    
    - parse the data in to the right format and order
    
    - create the time table and user table and insert to the corrosponding tables 
    
    """
    user_df_all=[]; time_df_all=[]
    # open log file
    df_log_file=pd.read_json(filepath,typ='series',lines = True) 
    length_lines=len(df_log_file)
    # iterate throguh log files
    for i, dict_log_file in enumerate(df_log_file):
        
        # extract the time information 
        time_data = [y for x,y in dict_log_file.items() if x=='ts']
        df=pd.DataFrame(np.array(time_data),columns=["time_stamp"])
       
        # convert timestamp column to datetime
        time_pd2=pd.to_datetime(df.time_stamp)       
        month = time_pd2.dt.month
        day = time_pd2.dt.day
        year = time_pd2.dt.year
        hour = time_pd2.dt.hour
        minute= time_pd2.dt.minute
        weekday= minute= time_pd2.dt.minute
        week=time_pd2.dt.week
        
        # insert time data records
        time_df=pd.DataFrame(np.array([df.time_stamp,hour,day,week,month,year,weekday]).T,columns=['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday'])
        time_df_all.append(time_df)
        
        # loading user table 
        users_table_data= [[x,y] for x,y in dict_log_file.items() if np.any([x==y for y in ["userId","firstName","lastName","gender","level"]])]       
        user_data_array=np.array(users_table_data)       
        user_df_temp = pd.DataFrame(np.array([user_data_array[:,1]]),columns=list(np.array(user_data_array)[:,0]))
        user_df_per_line=user_df_temp[["userId","firstName","lastName","gender","level"]]        
        if user_df_per_line["userId"].any():
            user_df_per_line["userId"]=user_df_per_line['userId'].astype(int)
            user_df_all.append(user_df_per_line)
        else:
            print ("######{:.2f}/{:.3f} SKIPPEND proccessing logfiles line".format(i,length_lines))
            continue
            

    # load time_data
    time_df=pd.concat(time_df_all)
    # load user table
    user_df=pd.concat(user_df_all)
    # insert the time table to db
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    k=0
    for row in df_log_file:
        k+=1       
        if row['song']==None or row['song']=="null":
            print(" ###########  data missing -->checking the values{} {} {}".format(row['song'], row['length'], row["artist"]))
            continue
        # extract data from db
        cur.execute(song_select, (row['song'],row['length'],))
        results_song_select = cur.fetchone()
        print("results_song_select--->{}".format(results_song_select))        
        cur.execute(artist_select, (row["artist"],))
        results_artist_select = cur.fetchone()
        print("results_artist_select--->{}".format(results_artist_select))

        if results_song_select:
            song_id, artist_id = results_song_select
        else:
            song_id, artist_id = None, None

        if results_artist_select:
            artist_id, name, location = results_artist_select
        else:
            artist_id, name, location = None, None, None
            
        if not  song_id or not artist_id:
            print ("SKIPPED song_id or artist_id nan values-->", song_id ,artist_id)
            continue
        # insert songplay record 
        song_play_data=[song_id,row["ts"], row["userId"], row["level"], artist_id, row["sessionId"], location, row["userAgent"]]       
        cur.execute(songplays_table_insert, song_play_data)

def process_data(cur, conn, filepath, func):
    """
    - process the data iterating through the files
    
    - feeds the available data for further processing log files and song file data
    
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main(host_dbname_user_info,song_data_path,log_file_path):
    """
    - Establishes connection with the sparkify database and gets cursor to it.  
    
    - taking the path of the song and log data extract, transform and load the song and log data
   
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    conn = psycopg2.connect(host_dbname_user_info)
    cur = conn.cursor()
    process_data(cur, conn, filepath=song_data_path, func=process_song_file)
    process_data(cur, conn, filepath=log_file_path, func=process_log_file)
    conn.close()
    
    
def get_files(filepath):
    """
    get all files in the given folder
    """
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files

if __name__ == "__main__":
    """
    run the main function for processing the song data as well as the log data 
    """
    host_dbname_user_info="host=127.0.0.1 dbname=sparkifydb user=student password=student"
    song_data_path='data/song_data'
    log_file_path='data/log_data'
    main(host_dbname_user_info,song_data_path,log_file_path)
    
   