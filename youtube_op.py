import sys
sys.path.append(r'C:\Users\Neethu\Downloads') 
from youtube_oop import Youtube

# Import necessary libraries
import streamlit as st
import cryptography
import pandas as pd
import pymysql
from streamlit_option_menu import option_menu
import time

# MySQL connection function for reuse
def connect_to_mysql():
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="Neethu0000",  # Replace with the correct password
            database="youtube"
        )
        return connection
    except pymysql.MySQLError as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

def data(channel_id):
    print(f"Fetching data for channel ID: {channel_id}")  # Add logging
    connect = Youtube(channel_id)  # Create an instance of the Youtube class
    
    channel = connect.channel()
    
    if not channel.empty:
        print("Channel data fetched, creating table and inserting data...")  # Add logging
        connect.channel_table(channel)
    else:
        print("No channel data fetched.")  # Log if no data is fetched

    videoid = connect.video_ids()
    video_details = connect.video_details(videoid)
    comments = connect.comment_details(videoid)
    
    connect.video_table(video_details)
    connect.comment_table(comments)


# Test the table creation by fetching data and inserting it
def test_channel_table_creation(channel_id):
    yt = Youtube("UCqECaJ8Gagnn7YCbPEzWH6g") 
    channel_data = yt.channel()  # Fetch channel details
    yt.channel_table(channel_data)  # Create the table and insert data
    st.write("Channel table created and data inserted successfully.")

# Streamlit App Code

st.image("C:/Users/Harsha/Downloads/youtube_logo.png", width=300)


# Sidebar Menu
with st.sidebar:
    select = option_menu("Menu", ['Home', 'channel details', 'Queries'], 
                         icons=["house", "pencil", "bar-chart"], 
                         orientation="vertical", 
                         styles={"container": {"padding": "5px"},
                                 "icon": {"color": "orange", "font-size": "18px"},
                                 "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px"}})


# Home Page
if select == "Home":
    st.title('Home')
    
    st.markdown("""
    <h1 style='text-align: center; color: #FF0000; font-size: 30px;'>Youtube Data Harvesting & Warehousing using SQL</h1>
    """, unsafe_allow_html=True)
    
    st.subheader(':red[Domain :] Social Media')

    st.markdown("""
    <h2 style='text-align: center; color: black;'>Project Overview</h2>
    <p style='text-align: justify; font-size: 18px;'>
        This project focuses on harvesting data from a YouTube channel using the YouTube API and organizing the collected data into a 
        structured format using pandas. The cleaned data is temporarily stored in dataframes before being transferred to a SQL-based data warehouse.
        The project involves collecting channel and video data and storing it in SQL tables. 
        SQL queries are then applied to retrieve meaningful insights from the warehouse and the results are displayed through a Streamlit-based 
        web application.
    </p>
    """, unsafe_allow_html=True)

    st.markdown("""
    <h3 style='color: #4CAF50;'>Skills & Technologies Utilized:</h3>
    <ul style='font-size: 18px;'>
        <li><b>Python</b>: Used for data processing and API integration</li>
        <li><b>YouTube API</b>: For extracting channel and video data</li>
        <li><b>Pandas</b>: Data manipulation and transformation</li>
        <li><b>SQL</b>: Data warehousing and executing queries</li>
        <li><b>Streamlit</b>: For building an interactive web application</li>
    </ul>
""", unsafe_allow_html=True)
    st.markdown("""
    <h3 style='color: #4CAF50;'>Project Benefits:</h3>
    <ul style='font-size: 18px;'>
        <li>Automates the process of extracting and storing YouTube data.</li>
        <li>Provides real-time insights from the data warehouse using SQL queries.</li>
        <li>Easy-to-use web interface for non-technical users to retrieve insights.</li>
    </ul>
""", unsafe_allow_html=True)
    st.markdown("""
    <h3 style='color: #4CAF50;'>Future Scope:</h3>
    <ul style='font-size: 18px;'>
        <li>Integrating more social media platforms for broader analysis.</li>
        <li>Using machine learning models for predicting video popularity.</li>
        <li>Adding more interactive visualizations for data insights.</li>
    </ul>
""", unsafe_allow_html=True)


# Channel Details Page
elif select == "channel details":
    st.title('Channel Details')

    # Input field for Channel ID
    channel_id = st.text_input(label='Enter a YouTube Channel ID')
    button = st.button(label='Fetch')

    if button:
        data(channel_id)

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

# SQL Queries Page
else:    
    st.write("Select SQL Queries to show the output")

    # Dropdown to select SQL Queries
    SQL_Queries = st.selectbox("SQL Queries",
                            ("Select Your Questions",
                             "1. What are the names of all the videos and their corresponding channels?",
                             "2. Which channels have the most number of videos, and how many videos do they have?",
                             "3. What are the top 10 most viewed videos and their respective channels?",
                             "4. How many comments were made on each video, and what are their corresponding video names?",
                             "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                             "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                             "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                             "8. What are the names of all the channels that have published videos in the year 2022?",
                             "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                             "10. Which videos have the highest number of comments, and what are their corresponding channel names?"))

    # MySQL connection to retrieve data based on selected queries
    my_connection = pymysql.connect(host="127.0.0.1", user="root", password="Neethu0000")
    mycursor = my_connection.cursor()
    mycursor.execute('USE youtube')

    # SQL Query Implementation for each selection
    if SQL_Queries == "1. What are the names of all the videos and their corresponding channels?":
        mycursor.execute("SELECT c.channel_name, v.video_name FROM video AS v INNER JOIN channel AS c ON v.channel_id = c.channel_id")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_name'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "2. Which channels have the most number of videos, and how many videos do they have?":
        mycursor.execute("SELECT channel_name, videos FROM channel ORDER BY videos DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Videos'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "3. What are the top 10 most viewed videos and their respective channels?":
        mycursor.execute("SELECT c.channel_name, v.video_name, v.views FROM video AS v INNER JOIN channel AS c ON v.channel_id = c.channel_id ORDER BY views DESC LIMIT 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Video_name', 'Most_viewed_video'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "4. How many comments were made on each video, and what are their corresponding video names?":
        mycursor.execute("SELECT video_name, comments FROM video ORDER BY comments DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Comments'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "5. Which videos have the highest number of likes, and what are their corresponding channel names?":
        mycursor.execute("SELECT c.channel_name, SUM(v.likes) AS Highest_likes FROM video AS v INNER JOIN channel AS c ON v.channel_id = c.channel_id GROUP BY c.channel_name ORDER BY Highest_likes DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_Likes'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
        mycursor.execute("SELECT video_name, likes FROM video ORDER BY likes DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Video_name', 'Likes'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
        mycursor.execute("SELECT channel_name, views FROM channel ORDER BY views DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Views'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "8. What are the names of all the channels that have published videos in the year 2022?":
        mycursor.execute("SELECT DISTINCT channel_name FROM channel AS c INNER JOIN video AS v ON v.channel_id = c.channel_id WHERE published_at BETWEEN '2022-01-01 00:00:00' AND '2022-12-31 23:59:59'")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        mycursor.execute("SELECT c.channel_name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(v.duration)))), '%H:%i:%s') AS average_duration FROM video AS v INNER JOIN channel AS c ON v.channel_id = c.channel_id GROUP BY c.channel_name")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Average_duration'])
        df.index += 1
        st.write(df)

    elif SQL_Queries == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
        mycursor.execute("SELECT c.channel_name, SUM(v.comments) AS Highest_comment FROM video AS v INNER JOIN channel AS c ON v.channel_id = c.channel_id GROUP BY c.channel_name ORDER BY Highest_comment DESC")
        df = pd.DataFrame(mycursor.fetchall(), columns=['Channel_name', 'Highest_comment'])
        df.index += 1
        st.write(df)

# Add attribution at the bottom of the page
st.markdown("""
    <hr>
    <h4 style='text-align: center;'>Created by Neethushree Kumar</h4>
    """, unsafe_allow_html=True)