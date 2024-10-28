import googleapiclient.discovery
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
from datetime import timedelta

# Youtube class definition
class Youtube:

    def __init__(self, channel_id):
        self.api_service_name = "youtube"
        self.api_version = "v3"
        self.youtube_api_key = "AIzaSyAAJGuRjLnpKi59PDK9TlAYet686Xc1cCE"  
        self.youtube = googleapiclient.discovery.build(self.api_service_name, self.api_version, developerKey=self.youtube_api_key)

        self.channel_id = channel_id

        self.db = pymysql.connect(
            host="localhost",
            user="root",
            password=Neethu0000",  # Replace with your actual MySQL password
            connect_timeout=10
        )

        self.mysql = "mysql+pymysql://root:Neethu0000@localhost/youtube"  # Replace with your MySQL connection details
        self.engine = create_engine(self.mysql)

        self.connect = self.db.cursor()

    # Fetch channel details
    def channel(self):
        try:
            request = self.youtube.channels().list(
                part="snippet,contentDetails,statistics",
                id=self.channel_id
            )
            channel_response = request.execute()

            if 'items' in channel_response and len(channel_response['items']) > 0:
                channel_data = {
                    "channel_id": channel_response["items"][0]["id"],
                    "channel_name": channel_response["items"][0]["snippet"]["title"],
                    "description": channel_response["items"][0]["snippet"]["description"],
                    "date": channel_response["items"][0]["snippet"]["publishedAt"],
                    "upload_id": channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
                    "views": channel_response["items"][0]["statistics"]["viewCount"],
                    "subscribers": channel_response["items"][0]["statistics"]["subscriberCount"],
                    "videos": channel_response["items"][0]["statistics"]["videoCount"]
                }
                df = pd.DataFrame(channel_data, index=[0])
                df['date'] = (pd.to_datetime(df['date'], utc=True)).dt.date
                df['date'] = pd.to_datetime(df['date'])
            else:
                df = pd.DataFrame()

        except Exception as e:
            df = pd.DataFrame()

        return df

    # Fetch video IDs
    def video_ids(self): 
        video_ids = []
        response = self.youtube.channels().list(part="contentDetails", id=self.channel_id).execute()

        # Handle missing keys
        try:
            playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        except KeyError as e:
            print(f"KeyError: {e}")
            return []

        next_page_token = None

        while True:
            playlist_response = self.youtube.playlistItems().list(
                part="snippet",
                maxResults=50,
                pageToken=next_page_token,
                playlistId=playlist_id
            ).execute()

            for item in playlist_response.get('items', []):
                video_ids.append(item['snippet']['resourceId']['videoId'])
            
            next_page_token = playlist_response.get('nextPageToken')

            if not next_page_token:
                break
        
        return video_ids

    # Fetch video details
    def video_details(self, video_ids):  
        video_details = []

        for video_id in video_ids:
            try:
                response = self.youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()

                for i in response['items']:
                    duration_seconds = pd.to_timedelta(i['contentDetails']['duration']).total_seconds()
                    data = {
                        "channel_id": i['snippet']['channelId'],
                        "video_id": i['id'],
                        "video_name": i['snippet']['title'],
                        "description": i['snippet']['description'],
                        "published_at": i['snippet']['publishedAt'],
                        "views": i['statistics']['viewCount'],
                        "likes": i['statistics'].get('likeCount'),
                        "dislikes": i.get('dislikeCount'),
                        "favorites": i['statistics']['favoriteCount'],
                        "comments": i['statistics'].get('commentCount'),
                        "duration": duration_seconds,
                        "thumbnail": i['snippet']['thumbnails']['default']['url'],
                        "caption_status": i['contentDetails']['caption'],
                        "tags": ",".join(i['snippet'].get('tags', ["na"]))
                    }
                    video_details.append(data)
            except Exception as e:
                print(f"An error occurred while fetching details for video ID {video_id}: {e}")
        
        if video_details:
            df = pd.DataFrame(video_details)
            df['published_at'] = pd.to_datetime(df['published_at'], format='%Y-%m-%dT%H:%M:%SZ')
            df['duration'] = pd.to_timedelta(df['duration'])
        else:
            df = pd.DataFrame()

        return df

    # Fetch comment details
    def comment_details(self, video_ids):  
        comment_details = []

        for video_id in video_ids:
            try:
                response = self.youtube.commentThreads().list(part="snippet", videoId=video_id, maxResults=20).execute()

                for item in response.get('items', []):
                    data = {
                        'comment_id': item['id'],
                        'video_id': item['snippet']['videoId'],
                        'text': item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        'author': item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        'published_at': item['snippet']['topLevelComment']['snippet']['publishedAt']
                    }
                    comment_details.append(data)
            except Exception as e:
                print(f"An error occurred while fetching comments for video ID {video_id}: {e}")

        df = pd.DataFrame(comment_details)
        
        if 'published_at' in df.columns:
            df['published_at'] = pd.to_datetime(df['published_at'], format='%Y-%m-%dT%H:%M:%SZ')
        else:
            print("Warning: 'published_at' column is missing from the comments DataFrame")
        
        return df
   def channel_table(self, channel_details):
    print("Switching to 'youtube' database and creating 'channel' table...")  # Log this step
    self.connect.execute('CREATE DATABASE IF NOT EXISTS youtube')  # Ensure the database exists
    self.connect.execute('USE youtube')  # Use the youtube database

    # Create 'channel' table if it doesn't exist
    self.connect.execute('''
        CREATE TABLE IF NOT EXISTS channel( 
            channel_id VARCHAR(50) PRIMARY KEY,
            channel_name VARCHAR(1000),
            description VARCHAR(1000), 
            date DATETIME,
            upload_id VARCHAR(50), 
            views BIGINT,
            subscribers BIGINT, 
            videos BIGINT
        )
    ''')
    print("Channel table created successfully.")  # Log table creation

    # Commit to ensure the table is created 
    self.db.commit()

    # Insert data into the channel table
    if not channel_details.empty:
        print("Inserting data into 'channel' table...")
        channel_details.to_sql('channel', self.engine, if_exists='append', index=False)
        print("Data inserted into 'channel' table successfully.")
    else:
        print("No channel data to insert.")



    # Save video details to MySQL
    def video_table(self, videos_details):
        self.connect.execute('create database if not exists youtube')
        self.connect.execute('use youtube')

        self.connect.execute('''create table if not exists video( 
            channel_id VARCHAR(50), 
            video_id VARCHAR(50) PRIMARY KEY,
            video_name VARCHAR(1000), 
            description VARCHAR(10000),
            published_at VARCHAR(50), 
            views BIGINT,
            likes BIGINT, 
            dislikes BIGINT, 
            favorites BIGINT, 
            comments BIGINT,
            duration VARCHAR(50), 
            thumbnail VARCHAR(100), 
            caption_status VARCHAR(50), 
            tags VARCHAR(1000),
            FOREIGN KEY (channel_id) REFERENCES channel(channel_id))''')

        self.db.commit()
        videos_details.to_sql('video', self.engine, if_exists='append', index=False)

    # Save comment details to MySQL
    def comment_table(self, comments_details):
        self.connect.execute('create database if not exists youtube')
        self.connect.execute('use youtube')

        self.connect.execute('''create table if not exists Comment(
            video_id VARCHAR(50), 
            comment_id VARCHAR(50),
            text TEXT,
            author VARCHAR(1000), 
            published_at DATETIME,
            FOREIGN KEY (video_id) REFERENCES video(video_id))''')

        self.db.commit()
        comments_details.to_sql('comment', self.engine, if_exists='append', index=False)
        self.engine.dispose()

# -------------------------------------------------------------
# TEST FUNCTION TO CREATE THE CHANNEL TABLE AND INSERT DATA
# -------------------------------------------------------------
# Channel Details Page
elif select == "channel details":
    st.title('Channel Details')

    # Input field for Channel ID
    channel_id = st.text_input(label='UCqECaJ8Gagnn7YCbPEzWH6g')
    button = st.button(label='Fetch')

    # Ensure that data is fetched and the table is created first
    if button:
        data(channel_id)  # This should create the 'channel' table and insert data

        # Progress bar for visual feedback
        bar = st.progress(0)
        for i in range(1, 11):
            bar.progress(i * 10)
            time.sleep(0.5)
        
        st.write('Channel details are collected')

    # Display Channel Data from MySQL database
    my_connection = pymysql.connect(host="127.0.0.1", user="root", password="Neethu0000", database='youtube')
    mycursor = my_connection.cursor()
    mycursor.execute("SELECT channel_name, channel_id, subscribers, videos FROM channel")
    
    channel_df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Channel_Id', 'Subscribers', 'Videos'])
    channel_df.drop_duplicates(inplace=True)
    channel_df.index += 1
    st.write(channel_df)






