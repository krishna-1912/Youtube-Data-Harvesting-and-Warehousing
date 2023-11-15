from googleapiclient.discovery import build
from pymongo import MongoClient
import datetime
import mysql.connector
import streamlit as st
import mysql.connector

# api key
api_key = "AIzaSyBMUD52Jov6a1LBkSCvZvyay4Ih0-7GPPk" 
youtube = build("youtube", "v3", developerKey=api_key)

# mongodb connection
mongo_client = MongoClient("mongodb://localhost:27017")
mongodb = mongo_client["youtube_data_harvesting"]
collection = mongodb["channelData"]

#mysql connection
mysql_database = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="youtube_data_harvesting"
)
cursor = mysql_database.cursor()


# Function to retrieve channel data
def extract_data(channel_ids):
    
    channel_data = youtube.channels().list(
        part="snippet,statistics",
        id=','.join(channel_ids)
    ).execute()

    i=0
    for channel in channel_data["items"]:
        try:
            channel_id = channel["id"]
            channel_name = channel["snippet"]["title"]
            subscribers = channel["statistics"]["subscriberCount"]
            total_videos = channel["statistics"]["videoCount"]

            # retrieve all the playlists associated with the channel
            playlists_data = youtube.playlists().list(
                part = "snippet",
                channelId = channel_id,
                maxResults = 50
            ).execute()

            playlist_ids = [playlist["id"] for playlist in playlists_data["items"]]

            # create the channel data document
            channel_data = {
                    "Channel_Name": channel_name,
                    "Channel_Id": channel_id,
                    "Subscription_Count": subscribers,
                    "Channel_Views": total_videos,
                    "Channel_Description": channel["snippet"]["description"],
                    "Playlist_Id": playlist_ids[0] if playlist_ids else None
                }

            # Retrieve the video data for each playlist
            video_data = []
            video = {}
            for playlist_id in playlist_ids:
                playlist_items = youtube.playlistItems().list(
                    part="snippet",
                    maxResults=50,
                    playlistId=playlist_id
                ).execute()

                for item in playlist_items["items"]:
                    try:
                        video_id = item["snippet"]["resourceId"]["videoId"]
                        video_info = youtube.videos().list(
                            part="snippet,statistics,contentDetails",
                            id=video_id
                        ).execute()

                        video_info = video_info["items"][0]["snippet"]
                        video_stats = video_info.get("statistics", {})

                        video_details = {
                                "video_id": video_id,
                                "video_name": video_info["title"],
                                "video_description": video_info["description"],
                                "tags": video_info.get("tags", []),
                                "published_at": datetime.datetime.strptime(video_info["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S"),
                                "view_count": video_stats.get("viewCount", 0),
                                "like_count": video_stats.get("likeCount", 0),
                                "dislike_count": video_stats.get("dislikeCount", 0),
                                "favorite_count": video_stats.get("favoriteCount", 0),
                                "comment_count": video_stats.get("commentCount", 0),
                                "duration": video_info.get("contentDetails", {}).get("duration", ""),
                                "thumbnail": video_info["thumbnails"]["default"]["url"],
                                "caption_status": video_info.get("contentDetails", {}).get("caption", {}).get("status", ""),
                                "comments": []
                            }
                        

                        # Retrieve comments
                        comments = youtube.commentThreads().list(
                            part="snippet",
                            videoId=video_id,
                            maxResults=10
                        ).execute()

                        for comment in comments["items"]:
                            comment_data = comment["snippet"]["topLevelComment"]["snippet"]
                            comment_details = {
                                "comment_id": comment["id"],
                                "comment_text": comment_data["textDisplay"],
                                "comment_author": comment_data["authorDisplayName"],
                                "comment_published_at": datetime.datetime.strptime(comment_data["publishedAt"], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y-%m-%d %H:%M:%S"),
                            }
                            video_details["comments"].append(comment_details)
                            
                            while True:
                                i=i+1
                                key = f'video{i}'
                                value = video_details
                                video[key] = value
                                break
                    except Exception as a:
                        print("something going wrong",a)
                        

            channel  ={"Channel_data": channel_data,
                    "video_data": video}
            if collection.find(channel_data["Channel_Id"]) :
                collection.update_one(channel)
            else :
                collection.insert_one(channel)
            
            video_data.append(video_details)
        
        except Exception as e:
            print("Exception occurred while inserting data in MongoDb", e)
                

# MYSQL Table creation
pl_id = ''
# channel table
channel_table_query = """
CREATE TABLE IF NOT EXISTS channel (
    channel_id VARCHAR(255) PRIMARY KEY,
    channel_name VARCHAR(255),
    subscription_count INT,
    channel_views INT,
    channel_description TEXT,
    playlist_id VARCHAR(255)
)
"""
cursor.execute(channel_table_query)

# video table
video_table_query = """
CREATE TABLE IF NOT EXISTS video (
    video_id VARCHAR(255) PRIMARY KEY,
    playlist_id VARCHAR(255),
    video_name VARCHAR(255),
    video_description TEXT,
    tags TEXT,
    published_at TIMESTAMP,
    view_count INT,
    like_count INT,
    dislike_count INT,
    favorite_count INT,
    comment_count INT,
    duration VARCHAR(255),
    thumbnail VARCHAR(255),
    caption_status VARCHAR(255)
)
"""
cursor.execute(video_table_query)

# playlist table
playlist_table_query = """
CREATE TABLE IF NOT EXISTS playlist (
    playlist_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255),
    FOREIGN KEY (channel_id) REFERENCES channel(channel_id)
)
"""
cursor.execute(playlist_table_query)

# comment table
comment_table_query = """
CREATE TABLE IF NOT EXISTS comment (
    comment_id VARCHAR(255) PRIMARY KEY,
    video_id VARCHAR(255),
    comment_text TEXT,
    comment_author VARCHAR(255),
    comment_published_at TIMESTAMP,
    FOREIGN KEY (video_id) REFERENCES video(video_id)
)
"""
cursor.execute(comment_table_query)


def export_to_mysql():

    cursor = mysql_database.cursor()
    # Retrieve channel data and insert into channel table
    for channel_data in collection.find():
        try:
            channel_query = """
            INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, channel_description, playlist_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            channel_values = (
                str(channel_data["_id"]),
                channel_data["Channel_data"]["Channel_Name"],
                channel_data["Channel_data"]["Subscription_Count"],
                channel_data["Channel_data"]["Channel_Views"],
                channel_data["Channel_data"]["Channel_Description"],
                channel_data["Channel_data"]["Playlist_Id"]
            ) 
            pl_id=channel_data["Channel_data"]["Playlist_Id"]
            cursor.execute(channel_query, channel_values)
            break
        except Exception as e:
            print(e)

    # Retrieve video data and insert into video table
    a = 0
    for video_data in collection.find():
        if a == 0:
            a += 1
            continue
        try:
            video_query = """
            INSERT INTO video (video_id,playlist_id, video_name, video_description, tags, published_at, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            video_values = (
                str(video_data["_id"]),
                pl_id,
                video_data["video_data"].get("video_name", "-"),
                video_data["video_data"].get("video_description", "-"),
                ", ".join(video_data["video_data"].get("tags", [])),
                video_data["video_data"].get("published_at", "-"),
                video_data["video_data"].get("view_count", 0),
                video_data["video_data"].get("like_count", 0),
                video_data["video_data"].get("dislike_count", 0),
                video_data["video_data"].get("favorite_count", 0),
                video_data["video_data"].get("comment_count", 0),
                video_data["video_data"].get("duration", "-"),
                video_data["video_data"].get("thumbnail", "-"),
                video_data["video_data"].get("caption_status", "-")
            )

            cursor.execute(video_query, video_values)
        except Exception as f:
            print(f)

    # Retrieve playlist data and insert into playlist table
    for channel_data in collection.find():
        playlist_query = """
        INSERT INTO playlist (playlist_id, channel_id)
        VALUES (%s, %s)
        """
        playlist_values = (
            channel_data["Channel_data"]["Playlist_Id"],
            str(channel_data["Channel_data"]["Channel_Id"])
        )
        cursor.execute(playlist_query, playlist_values)
        break

    # Retrieve comment data and insert into comment table
    b = 0
    for video_data in collection.find():
        if b == 0:
            b += 1
            continue
        try:
            for comment_data in video_data["video_data"]["comments"]:
                comment_query = """
                INSERT INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_at)
                VALUES (%s, %s, %s, %s, %s)
                """
                comment_values = (
                    comment_data["comment_id"],
                    str(video_data["_id"]),
                    comment_data["comment_text"],
                    comment_data["comment_author"],
                    comment_data["comment_published_at"]
                )
                cursor.execute(comment_query, comment_values)
        except Exception as e:
            print(e)

    mysql_database.commit()
    cursor.close()
    mysql_database.close()


# main streamlit 
def main():
    st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
    
    #Enter youtube channel id
    channel_id = st.text_input("Enter Channel ID:")

    # Export to MongoDB and Migrate to MySQL button
    if st.button("Export and Migrate"):
        extract_data([channel_id])
        export_to_mysql()

    st.header("SQL Query Output")

    # SQL query selection dropdown
    query_selection = st.selectbox("Select a Query", [
        "What are the names of all the videos and their corresponding channels?",
        "Which channels have the most number of videos, and how many videos do they have?",
        "What are the top 10 most viewed videos and their respective channels?",
        "How many comments were made on each video, and what are their corresponding video names?",
        "Which videos have the highest number of likes, and what are their corresponding channel names?",
        "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
        "What is the total number of views for each channel, and what are their corresponding channel names?",
        "What are the names of all the channels that have published videos in the year 2022?",
        "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "Which videos have the highest number of comments, and what are their corresponding channel names?"
    ])

    # Execute SQL query button
    if st.button("Execute SQL Query"):
        connect = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root",
            database="youtube_data_harvesting"
        )
        cursor = connect.cursor()

        # Execute the selected SQL query
        if query_selection == "What are the names of all the videos and their corresponding channels?":
            query = "SELECT v.video_name, c.channel_name FROM yt_db.video v JOIN yt_db.channel c ON v.playlist_id = c.playlist_id;"
        elif query_selection == "Which channels have the most number of videos, and how many videos do they have?":
            query = "SELECT c.channel_id, c.channel_name, COUNT(v.video_id) AS video_count FROM yt_db.channel c JOIN yt_db.playlist p ON c.channel_id = p.channel_id JOIN yt_db.video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_id, c.channel_name ORDER BY video_count DESC;"
        elif query_selection == "What are the top 10 most viewed videos and their respective channels?":
            query = "SELECT v.video_name, c.channel_name, v.view_count FROM yt_db.video v JOIN yt_db.playlist p ON v.playlist_id = p.playlist_id JOIN yt_db.channel c ON p.channel_id = c.channel_id ORDER BY v.view_count DESC LIMIT 10;"
        elif query_selection == "How many comments were made on each video, and what are their corresponding video names?":
            query = "SELECT v.video_name, COUNT(c.comment_id) AS comment_count FROM yt_db.video v LEFT JOIN yt_db.comment c ON v.video_id = c.video_id GROUP BY v.video_name;"
        elif query_selection == "Which videos have the highest number of likes, and what are their corresponding channel names?":
            query = "SELECT v.video_name, c.channel_name, v.like_count FROM yt_db.video v JOIN yt_db.playlist p ON v.playlist_id = p.playlist_id JOIN yt_db.channel c ON p.channel_id = c.channel_id ORDER BY v.like_count DESC;"
        elif query_selection == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
            query = "SELECT v.video_name, SUM(v.like_count) AS total_likes, SUM(v.dislike_count) AS total_dislikes FROM yt_db.video v GROUP BY v.video_name;"
        elif query_selection == "What is the total number of views for each channel, and what are their corresponding channel names?":
            query = "SELECT c.channel_name, SUM(v.view_count) AS total_views FROM yt_db.channel c JOIN yt_db.playlist p ON c.channel_id = p.channel_id JOIN yt_db.video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name;"
        elif query_selection == "What are the names of all the channels that have published videos in the year 2022?":
            query = "SELECT DISTINCT c.channel_name FROM yt_db.channel c JOIN yt_db.playlist p ON c.channel_id = p.channel_id JOIN yt_db.video v ON p.playlist_id = v.playlist_id WHERE YEAR(v.published_at) = 2022;"
        elif query_selection == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query = "SELECT c.channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(v.duration))) AS average_duration FROM yt_db.channel c JOIN yt_db.playlist p ON c.channel_id = p.channel_id JOIN yt_db.video v ON p.playlist_id = v.playlist_id GROUP BY c.channel_name;"
        elif query_selection == "Which videos have the highest number of comments, and what are their corresponding channel names?":
            query = "SELECT v.video_name, ch.channel_name, COUNT(co.comment_id) AS comment_count FROM yt_db.video v JOIN yt_db.playlist p ON v.playlist_id = p.playlist_id JOIN yt_db.channel ch ON p.channel_id = ch.channel_id JOIN yt_db.comment co ON v.video_id = co.video_id GROUP BY v.video_name, ch.channel_name ORDER BY comment_count DESC;"

        cursor.execute(query)

        # Fetch all rows of the query result
        rows = cursor.fetchall()

        # Display query result as a table
        st.table(rows)

        cursor.close()
        connect.close()


if __name__ == "__main__":
    main()
