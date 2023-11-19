from googleapiclient.discovery import build
from datetime import datetime
import time
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import pymongo as pg
import streamlit as st

# api key initialization
API_KEY ='AIzaSyD4e8MzDM9ZH1l4e8_RLi-U9L9gTbiSxCk'
youtube = build('youtube', 'v3', developerKey=API_KEY)

# mongodb connection
DATABASE = 'youtube_data_harvesting'
mongoclient = pg.MongoClient("mongodb://localhost:27017")
db  = mongoclient[DATABASE]
col1 = db["channel_data"]
col2 = db["video_data"]
col3 = db["comment_data"]

# mysql connection
mysqlClient = mysql.connector.connect(
    host="localhost",
    port=3306,
    user="root",
    password="root",
    database=DATABASE
)

cursor = mysqlClient.cursor()
engine = create_engine('mysql+mysqlconnector://root:root@localhost/youtube_data_harvesting')

# get channel details
@st.cache_data
def channel_details(_youtube,channel_ids):
    channel_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_ids)
    response = request.execute()

    for i in range(len(response["items"])):
        data = dict(channel_id = response["items"][i]["id"],
                    channel_name = response["items"][i]["snippet"]["title"],
                    channel_views = response["items"][i]["statistics"]["viewCount"],
                    subscriber_count = response["items"][i]["statistics"]["subscriberCount"],
                    total_videos = response["items"][i]["statistics"]["videoCount"],
                    playlist_id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        channel_data.append(data)
    return channel_data

# get playlist data
@st.cache_data
def get_playlist_details(df):
    playlist_ids = []
    for i in df["playlist_id"]:
        playlist_ids.append(i)

    return playlist_ids

# get video ids
@st.cache_data
def get_video_ids(_youtube,playlist_id_data):
    video_id = []

    for i in playlist_id_data:
        next_page_token = None
        more_pages = True

        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
            
            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])
        
            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return video_id
        
# get video details
@st.cache_data
def get_video_details(_youtube,video_id):

    all_video_stats = []

    for i in range(0,len(video_id),50):
        
        request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id = ",".join(video_id[i:i+50]))
        response = request.execute()
        
        for video in response["items"]:
            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')

            videos = dict(video_id = video["id"],
                          channel_id = video["snippet"]["channelId"],
                         video_name = video["snippet"]["title"],
                         published_date = format_date ,
                         view_count = video["statistics"].get("viewCount",0),
                         like_count = video["statistics"].get("likeCount",0),
                         dislike_count = video["statistics"].get("dislikeCount",0),
                         comment_count= video["statistics"].get("commentCount",0),
                         duration = video["contentDetails"]["duration"])
            all_video_stats.append(videos)

    return (all_video_stats)

# get comment details
@st.cache_data
def get_comments(_youtube,video_ids):
    comments_data= []
    try:
        next_page_token = None
        for i in video_ids:
            while True:
                request = youtube.commentThreads().list(
                    part = "snippet,replies",
                    videoId = i,
                    textFormat="plainText",
                    maxResults = 100,
                    pageToken=next_page_token)
                response = request.execute()

                for item in response["items"]:
                    published_date= item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                    parsed_dates = datetime.strptime(published_date,'%Y-%m-%dT%H:%M:%SZ')
                    format_date = parsed_dates.strftime('%Y-%m-%d')
                    

                    comments = dict(comment_id = item["id"],
                                    video_id = item["snippet"]["videoId"],
                                    comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    comment_author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    comment_published_date = format_date)
                    comments_data.append(comments) 
                
                next_page_token = response.get('nextPageToken')
                if next_page_token is None:
                    break       
    except Exception as e:
        print("An error occured",str(e))          
            
    return comments_data
  
# streamlit
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing", page_icon="▶", layout="wide", initial_sidebar_state="auto")
st.markdown("<h1 style='text-align: center; color: violet;'>YOUTUBE DATA HARVESTING AND WAREHOUSING</h1>", unsafe_allow_html=True)
st.subheader(":black[Uploading Data to MongoDB Database]")

channel_ids = st.text_input("Enter youtube channel Id")
st.markdown("<p style='font-style: italic; color: green;'>Hint: Go to any youtube channel -> about -> share channel -> copy channel Id</p>", unsafe_allow_html=True)

channel_list = [channel_ids]
upload_to_mongodb = st.button("Upload to MongoDB Database")

if upload_to_mongodb:
    if channel_ids:
        channel_details = channel_details(youtube,channel_ids)
        df = pd.DataFrame(channel_details) 
        playlist_id_data = get_playlist_details(df)
        video_id = get_video_ids(youtube,playlist_id_data)
        video_details = get_video_details(youtube,video_id)
        get_comment_data = get_comments(youtube,video_id)
        

        with st.spinner('Fetch Data from youtube..'):
            time.sleep(5)
            st.success('Success!, Data Fetched Successfully')
        

            if channel_details:
                col1.insert_many(channel_details) 
            if video_details:
                col2.insert_many(video_details)
            if get_comment_data:
                col3.insert_many(get_comment_data)

        with st.spinner('Uploading Data to MongoDB...'):
            time.sleep(5)
            st.success('Sucess!, Data Uploaded Successfully')
            st.balloons()


def channel_names():   
    ch_name = []
    for i in db.channel_data.find():
        ch_name.append(i['channel_name'])
    return ch_name

st.subheader(":black[Transform data to MYSQL]")
user_input = st.multiselect("Select the channel to be inserted into MySQL",options = channel_names())
transform_to_sql = st.button("Tranform data into MySQL")

if transform_to_sql:

    with st.spinner('Please wait '):
        
        def get_channel_details(user_input):
            query = {"channel_name":{"$in":list(user_input)}}
            data = {"_id":0,"channel_id":1,"channel_name":1,"channel_views":1,"subscriber_count":1,"total_videos":1,"playlist_id":1}
            x = col1.find(query,data)
            channel_table = pd.DataFrame(list(x))
            return channel_table

        channel_data = get_channel_details(user_input)
        print(channel_data)
        
    
        def get_video_details(channel_list):
            query = {"channel_id":{"$in":channel_list}}
            data ={"_id":0,"video_id":1,"channel_id":1,"video_name":1,"published_date":1,"view_count":1,"like_count":1,"comment_count":1,"duration":1}
            x = col2.find(query,data)
            video_table = pd.DataFrame(list(x))
            return video_table

        video_data = get_video_details(channel_list)
        print(video_data)
    
        def get_comment_details(video_ids):
            query = {"video_id":{"$in":video_ids}}
            projection = {"_id":0,"comment_id":1,"video_id":1,"comment_text":1,"comment_author":1,"comment_published_date":1}
            x = col3.find(query,projection)
            comment_table = pd.DataFrame(list(x))
            return comment_table

        video_ids = video_data["video_id"].to_list()
       
        comment_data = get_comment_details(video_ids)
        st.write(comment_data)
        
        mongoclient.close()

        try:
            channel_data.to_sql('channel_data', con=engine, if_exists='append', index=False, method='multi')
            print("Data inserted successfully")
        except Exception as e:
            if 'Duplicate entry' in str(e):
                print("Duplicate data found. Ignoring duplicate entries.")
            else:
                print("An error occurred:", e)


        try:
            video_data.to_sql('video_data', con=engine, if_exists='append', index=False, method='multi')
            print("Data inserted successfully")
        except Exception as e: 
            if 'Duplicate entry' in str(e):
                print("Duplicate data found. Ignoring duplicate entries.")
            else:
                print("An error occurred:", e)
        st.success("Data Uploaded Successfully")

        engine.dispose()


        try:
            comment_data.to_sql('comment_data', con=engine, if_exists='append', index=False, method='multi')
            print("Data inserted successfully")
        except Exception as e: 
            if 'Duplicate entry' in str(e):
                print("Duplicate data found. Ignoring duplicate entries.")
            else:
                print("An error occurred:", e)
        st.success("Data Uploaded Successfully")

        engine.dispose()

st.subheader(":black[Select queries]")

# options to select queries
questions = st.selectbox("Select any questions given below:",
['Click the question that you would like to query',
'1. What are the names of all the videos and their corresponding channels?',
'2. Which channels have the most number of videos, and how many videos do they have?',
'3. What are the top 10 most viewed videos and their respective channels?',
'4. How many comments were made on each video, and what are their corresponding video names?',
'5. Which videos have the highest number of likes, and what are their corresponding channel names?',
'6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
'7. What is the total number of views for each channel, and what are their corresponding channel names?',
'8. What are the names of all the channels that have published videos in the year 2022?',
'9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
'10. Which videos have the highest number of comments, and what are their corresponding channel names?'])


# queries
if questions == '1. What are the names of all the videos and their corresponding channels?':
    query1 = """
            SELECT channel_data.channel_name AS Channel_Name, video_data.video_name AS Video_Name
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id;
            """
    cursor.execute(query1)

    # storing the results in Pandas Dataframe
    result1 = cursor.fetchall()
    table1 = pd.DataFrame(result1,columns = cursor.column_names)
    st.table(table1)

elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
    query2 = """
            SELECT channel_name AS Channel_Name, COUNT(video_id) AS Video_Count
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            GROUP BY channel_name
            ORDER BY video_count DESC;
            """
    cursor.execute(query2)
    result2 = cursor.fetchall()
    table2 = pd.DataFrame(result2,columns =cursor.column_names)
    st.table(table2)
    st.bar_chart(table2.set_index("Channel_Name"))

elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
    query3 = """
            SELECT video_name AS Video_Name, channel_name AS Channel_Name, view_count AS View_Count
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            ORDER BY view_count DESC
            LIMIT 10;
            """
    cursor.execute(query3)
    result3 = cursor.fetchall()
    table3 = pd.DataFrame(result3,columns=cursor.column_names)
    st.table(table3)
   

elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
    query4 = """
            SELECT video_name, COUNT(comment_id) AS comment_count
            FROM video_data
            JOIN comment_data ON video_data.video_id = comment_data.video_id
            GROUP BY video_name;
            """
    cursor.execute(query4)
    result4 = cursor.fetchall()
    table4 = pd.DataFrame(result4,columns=cursor.column_names)
    st.table(table4)
    

elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    query5 = """
            SELECT video_name AS Video_Name, channel_name AS Channel_Name, like_count AS Like_Count
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            ORDER BY like_count DESC
            LIMIT 10;
            """
    cursor.execute(query5)
    result5 = cursor.fetchall()
    table5 = pd.DataFrame(result5,columns=cursor.column_names)
    st.table(table5)
    

elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    query6 = """
            SELECT video_name AS Video_Name, SUM(like_count) AS Total_Likes
            FROM video_data
            GROUP BY video_name;
            """
    cursor.execute(query6)
    result6 = cursor.fetchall()
    table6 = pd.DataFrame(result6,columns=cursor.column_names)
    st.table(table6)

elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    query7 = """
            SELECT channel_name AS Channel_Name, SUM(view_count) AS Total_Views
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            GROUP BY channel_name;
            """
    cursor.execute(query7)
    result7 = cursor.fetchall()
    table7 = pd.DataFrame(result7,columns=cursor.column_names)
    st.table(table7)
  
elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
    query8 = """
            SELECT DISTINCT channel_name as Channel_Name
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            WHERE YEAR(published_date) = 2022;
            """
    cursor.execute(query8)
    result8 = cursor.fetchall()
    table8 = pd.DataFrame(result8,columns=cursor.column_names)
    st.table(table8)
    
elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    query9 = """
            SELECT channel_name AS Channel_Name, AVG(duration) AS Average_Duration
            FROM video_data
            JOIN channel_data ON video_data.channel_id = channel_data.channel_id
            GROUP BY channel_name;
            """
    cursor.execute(query9)
    result9 = cursor.fetchall()
    table9 = pd.DataFrame(result9,columns=cursor.column_names)
    

elif questions =='10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    query10 = "select channel_name as Channel_name,video_name as Video_name,comment_count as Highest_No_of_comments from channel_data c join video_data v on c.channel_id = v.channel_id order by comment_count desc limit 10;"
    cursor.execute(query10)
    result10 = cursor.fetchall()
    table10 = pd.DataFrame(result10,columns=cursor.column_names)
    st.table(table10)

# closing mysql connection
cursor.close()
mysqlClient.close()
