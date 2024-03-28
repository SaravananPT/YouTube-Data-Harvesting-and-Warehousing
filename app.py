import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st
import pprint


class YouTubeChannelAnalyzer:
    def __init__(self):
        self.youtube = None  # Initialize youtube object

    def authenticate(self, api_key):
        self.youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)

    def get_channel_id(self, channel_name):
        try:
            request = self.youtube.search().list(
                part="id",
                q=channel_name,
                type="channel",
                maxResults=1
            )
            response = request.execute()
            if 'items' in response:
                channel_id = response['items'][0]['id']['channelId']
                return channel_id
            else:
                return None
        except HttpError as e:
            print("An error occurred:", e)
            return None

    def get_channel_details(self, channel_id):
        try:
            request = self.youtube.channels().list(
                part="snippet,statistics,status",
                id=channel_id
            )
            response = request.execute()
            if 'items' in response:
                item = response['items'][0]
                channel_info = {
                    'channel_id': channel_id,
                    'channel_name': item['snippet']['title'],
                    'channel_type': item['snippet'].get('channelType', 'N/A'),
                    'channel_status': item['status'].get('privacyStatus', 'N/A'),
                    'video_count': item['statistics'].get('videoCount', 'N/A'),
                    'view_count': item['statistics'].get('viewCount', 'N/A'),
                    'subs_count': item['statistics'].get('subscriberCount', 'N/A'),
                    'publish_date': item['snippet'].get('publishedAt', 'N/A').split('T')[0].replace('-', ''),
                    'description': item['snippet'].get('description', 'N/A'),
                    'hidden_subs_count': item['statistics'].get('hiddenSubscriberCount', False)
                }
                return channel_info
            else:
                return None
        except HttpError as e:
            print("An error occurred:", e)
            return None

    def get_all_playlist_ids(self, channel_id):
        playlists_info = []
        next_page_token = None
        while True:
            try:
                request = self.youtube.playlists().list(
                    part="snippet",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                if 'items' in response:
                    for item in response['items']:
                        playlist_info = {
                            'channel_id': channel_id,
                            'playlist_id': item['id'],
                            'playlist_name': item['snippet']['title']
                        }
                        playlists_info.append(playlist_info)
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break
                else:
                    break
            except HttpError as e:
                print("An error occurred:", e)
                break
        return playlists_info

    def video_ids_from_playlist(self, playlist_ids):
        video_ids = set()
        for playlist_id in playlist_ids:
            next_page_token = None
            while True:
                try:
                    request = self.youtube.playlistItems().list(
                        part="contentDetails",
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    if 'items' in response:
                        for item in response['items']:
                            video_id = item['contentDetails']['videoId']
                            video_ids.add(video_id)
                        next_page_token = response.get('nextPageToken')
                        if not next_page_token:
                            break
                    else:
                        break
                except HttpError as e:
                    print("An error occurred:", e)
                    break
        return list(video_ids)

    def video_ids_from_channel(self, channel_id):
        video_ids = set()
        next_page_token = None
        while True:
            try:
                request = self.youtube.search().list(
                    part="id",
                    channelId=channel_id,
                    type="video",
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                if 'items' in response:
                    for item in response['items']:
                        video_ids.add(item['id']['videoId'])
                    next_page_token = response.get('nextPageToken')
                    if not next_page_token:
                        break
                else:
                    break
            except HttpError as e:
                print("An error occurred:", e)
                break
        return list(video_ids)

    def get_video_details(self, video_ids, channel_id):
        video_details = []
        for video_id in video_ids:
            try:
                request = self.youtube.videos().list(
                    part="snippet,statistics,contentDetails",
                    id=video_id
                )
                response = request.execute()
                if 'items' in response:
                    for item in response['items']:
                        title = item['snippet']['title']
                        description = item['snippet'].get('description', 'N/A')
                        published_at = item['snippet'].get('publishedAt', 'N/A').replace('Z', '').replace('T', ' ')
                        view_count = item['statistics'].get('viewCount', 'N/A')
                        like_count = item['statistics'].get('likeCount', 'N/A')
                        dislike_count = item['statistics'].get('dislikeCount')
                        if dislike_count == 'N/A':
                            dislike_count = None
                        comment_count = item['statistics'].get('commentCount', 'N/A')
                        favorite_count = item['statistics'].get('favoriteCount', 'N/A')
                        duration = item['contentDetails']['duration']
                        duration = duration[2:].replace('H', ':').replace('M', ':').replace('S', '')
                        thumbnail_url = item['snippet']['thumbnails']['default']['url']
                        caption_status = item['contentDetails'].get('caption', 'N/A')

                        video_details.append({
                            "channel_id": channel_id,
                            "video_id": video_id,
                            "title": title,
                            "description": description,
                            "published_at": published_at,
                            "view_count": view_count,
                            "like_count": like_count,
                            "dislike_count": dislike_count,
                            "comment_count": comment_count,
                            "favorite_count": favorite_count,
                            "duration": duration,
                            "thumbnail_url": thumbnail_url,
                            "caption_status": caption_status
                        })
            except HttpError as e:
                print("An error occurred:", e)
                break
        return video_details

    def get_video_comments(self, video_ids):
        video_comments = []
        for video_id in video_ids:
            next_page_token = None
            while True:
                try:
                    request = self.youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=next_page_token
                    )
                    response = request.execute()
                    if 'items' in response:
                        for item in response['items']:
                            comment_id = item['snippet']['topLevelComment']['id']
                            commenter_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
                            comment_published_at = (item['snippet']['topLevelComment']['snippet']
                                                    .get('publishedAt', 'N/A').replace('Z', '').replace('T', ' '))

                            video_comments.append({
                                "comment_id": comment_id,
                                "video_id": video_id,
                                "commenter_name": commenter_name,
                                "comment_text": comment_text,
                                "comment_published_at": comment_published_at
                            })
                        next_page_token = response.get('nextPageToken')
                        if not next_page_token:
                            break
                    else:
                        break
                except HttpError as e:
                    print("An error occurred:", e)
                    break
        return video_comments

    def analyze_channels(self, channel_names):
        output = {}
        for channel_name in channel_names:
            channel_id = self.get_channel_id(channel_name)
            if channel_id:
                output[channel_name] = {}
                output[channel_name]['channel_id'] = channel_id
                playlist_ids = self.get_all_playlist_ids(channel_id)
                if playlist_ids:
                    output[channel_name]['playlist_ids'] = playlist_ids
                    video_ids = self.video_ids_from_playlist([playlist['playlist_id'] for playlist in playlist_ids])
                    output[channel_name]['video_ids'] = video_ids

                    video_details = self.get_video_details(video_ids, channel_id)
                    output[channel_name]['video_details'] = video_details

                    video_comments = self.get_video_comments(video_ids)
                    output[channel_name]['video_comments'] = video_comments

                    video_ids = self.video_ids_from_channel(channel_id)
                    output[channel_name]['video_ids'] = video_ids

                    video_details = self.get_video_details(video_ids, channel_id)
                    output[channel_name]['video_details'] = video_details

                    video_comments = self.get_video_comments(video_ids)
                    output[channel_name]['video_comments'] = video_comments
                else:
                    video_ids = self.video_ids_from_channel(channel_id)
                    output[channel_name]['video_ids'] = video_ids

                    video_details = self.get_video_details(video_ids, channel_id)
                    output[channel_name]['video_details'] = video_details

                    video_comments = self.get_video_comments(video_ids)
                    output[channel_name]['video_comments'] = video_comments

                channel_details = self.get_channel_details(channel_id)
                output[channel_name]['channel_details'] = channel_details
            else:
                output[channel_name] = "Channel not found."
        pprint.pprint(output)
        return output

    def insert_data_to_mongodb(self, output, mongo_uri, mongodb_db_name):
        try:
            mongo_client = pymongo.MongoClient(mongo_uri)
            db = mongo_client[mongodb_db_name]

            for channel_name, data in output.items():
                try:
                    db.channels.insert_one(data['channel_details'])

                    if 'playlist_ids' in data:
                        db.playlists.insert_many(data['playlist_ids'])

                    if 'video_details' in data:
                        db.videos.insert_many(data['video_details'])

                    if 'video_comments' in data and isinstance(data['video_comments'], list) and data['video_comments']:
                        db.comments.insert_many(data['video_comments'])

                    print(channel_name, '- all details inserted to MongoDB Atlas')
                except Exception as e:
                    print(f"An error occurred while inserting data for channel '{channel_name}':", e)

        except Exception as e:
            print("An error occurred while inserting data to MongoDB Atlas:", e)

    def create_mysql_tables(self, host, user, password, database):
        # Establish connection to the  AWS MySQL database
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Create cursor object to execute SQL commands
        cursor = connection.cursor()

        # Define SQL statements to create tables
        create_channels_table = """
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            channel_type VARCHAR(50),
            channel_status VARCHAR(50),
            video_count INT,
            view_count INT,
            subs_count INT,
            publish_date DATE,
            description TEXT,
            hidden_subs_count BOOLEAN
        )
        """

        create_playlists_table = """
        CREATE TABLE IF NOT EXISTS playlists (
            channel_id VARCHAR(255),
            playlist_id VARCHAR(255) PRIMARY KEY,
            playlist_name VARCHAR(255),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
        """

        create_videos_table = """
        CREATE TABLE IF NOT EXISTS videos (
            channel_id VARCHAR(255),
            video_id VARCHAR(255) PRIMARY KEY,
            title VARCHAR(255),
            description TEXT,
            published_at DATETIME,
            view_count INT,
            like_count INT,
            dislike_count INT,
            comment_count INT,
            favorite_count INT,
            duration VARCHAR(50),
            thumbnail_url VARCHAR(255),
            caption_status VARCHAR(50),
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
        )
        """

        create_comments_table = """
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255),
            commenter_name VARCHAR(255),
            comment_text TEXT,
            comment_published_at DATETIME,
            FOREIGN KEY (video_id) REFERENCES videos(video_id)
        )
        """
        # Execute SQL commands to create tables
        cursor.execute(create_channels_table)
        cursor.execute(create_playlists_table)
        cursor.execute(create_videos_table)
        cursor.execute(create_comments_table)

        # Commit the transaction and close cursor/connection
        connection.commit()
        cursor.close()
        connection.close()

        print("Tables created successfully in AWS RDS MySQL database.")

    def import_data_to_mysql(self, mongo_uri, mongodb_db_name, host, user, password, database):
        try:
            # Connect to MongoDB Atlas
            self.mongo_client = pymongo.MongoClient(mongo_uri)
            self.mongo_db = self.mongo_client[mongodb_db_name]

            # Connect to AWS MySQL
            self.mysql_connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )
            self.mysql_cursor = self.mysql_connection.cursor()

            # Fetch data from MongoDB Atlas
            channels_data = self.mongo_db.channels.find({})
            playlists_data = self.mongo_db.playlists.find({})
            videos_data = self.mongo_db.videos.find({})
            comments_data = self.mongo_db.comments.find({})

            # Insert channel data into AWS MySQL
            for channel in channels_data:
                try:
                    self.mysql_cursor.execute("""
                        INSERT INTO channels (channel_id, channel_name, channel_type, channel_status, video_count,
                                                view_count, subs_count, publish_date, description, hidden_subs_count)
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                              (
                                                  channel['channel_id'],
                                                  channel['channel_name'],
                                                  channel['channel_type'],
                                                  channel['channel_status'],
                                                  channel['video_count'],
                                                  channel['view_count'],
                                                  channel['subs_count'],
                                                  channel['publish_date'],
                                                  channel['description'],
                                                  channel['hidden_subs_count']
                                              ))
                    self.mysql_connection.commit()
                except Exception as e:
                    print(f"Error inserting channel data: {e}")

            # Insert playlist data into AWS MySQL
            for playlist in playlists_data:
                try:
                    self.mysql_cursor.execute("""
                        INSERT INTO playlists (channel_id, playlist_id, playlist_name)
                                                VALUES (%s, %s, %s)""",
                                              (
                                                  playlist['channel_id'],
                                                  playlist['playlist_id'],
                                                  playlist['playlist_name']
                                              ))
                    self.mysql_connection.commit()
                except Exception as e:
                    print(f"Error inserting playlist data: {e}")

            # Insert video data into AWS MySQL
            # Insert video data into AWS MySQL
            for video in videos_data:
                try:
                    # Replace None values with appropriate defaults
                    like_count = video['like_count'] if video['like_count'] is not None else 0
                    dislike_count = video['dislike_count'] if video['dislike_count'] is not None else 0
                    comment_count = video['comment_count'] if video['comment_count'] is not None else 0
                    favorite_count = video['favorite_count'] if video['favorite_count'] is not None else 0

                    self.mysql_cursor.execute("""
                        INSERT INTO videos (channel_id, video_id, title, description, published_at, view_count,
                                            like_count, dislike_count, comment_count, favorite_count, duration,
                                            thumbnail_url, caption_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        video['channel_id'],
                        video['video_id'],
                        video['title'],
                        video['description'],
                        video['published_at'],
                        video['view_count'],
                        like_count,
                        dislike_count,
                        comment_count,
                        favorite_count,
                        video['duration'],
                        video['thumbnail_url'],
                        video['caption_status']
                    ))
                    self.mysql_connection.commit()
                except Exception as e:
                    print(f"Error inserting video data: {e}")

            # Insert comment data into AWS MySQL
            for comment in comments_data:
                try:
                    video_id = comment['video_id']
                    # Check if the video_id exists in the videos table
                    self.mysql_cursor.execute("SELECT COUNT(*) FROM videos WHERE video_id = %s",
                                              (video_id,))
                    video_exists = self.mysql_cursor.fetchone()[0]
                    if video_exists:
                        self.mysql_cursor.execute("""
                            INSERT INTO comments (comment_id, video_id, commenter_name, comment_text,
                             comment_published_at)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            comment['comment_id'],
                            video_id,
                            comment['commenter_name'],
                            comment['comment_text'],
                            comment['comment_published_at']
                        ))
                        self.mysql_connection.commit()
                    else:
                        print(f"Error inserting comment data: Video with video_id {video_id} does not exist.")
                except Exception as e:
                    print(f"Error inserting comment data: {e}")
        finally:
            if 'self.mysql_connection' in locals() and self.mysql_connection.is_connected():
                self.mysql_cursor.close()
                self.mysql_connection.close()

    def select_and_execute_queries(self, host, user, password, database):
        # Define the list of queries
        queries = {
            "1. What are the names of all the videos and their corresponding channels?": """
            SELECT a.channel_name, b.title AS video_names 
            FROM channels a 
            INNER JOIN videos b ON a.channel_id=b.channel_id;
            """,
            "2. Which channels have the most number of videos, and how many videos do they have?": """
            SELECT channel_name, video_count 
            FROM channels 
            WHERE video_count IN (SELECT MAX(video_count) 
            FROM channels);
            """,
            "3. What are the top 10 most viewed videos and their respective channels?": """
            SELECT a.channel_name, b.title ,b.view_count
            FROM channels a 
            INNER JOIN videos b ON a.channel_id = b.channel_id 
            ORDER BY b.view_count DESC 
            LIMIT 10;
            """,
            "4. How many comments were made on each video, and what are their corresponding video names?": """
            SELECT a.channel_name, b.video_id, b.title, b.comment_count 
            FROM channels a 
            INNER JOIN videos b ON a.channel_id=b.channel_id 
            ORDER BY comment_count DESC;
            """,
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT a.channel_name, b.title, b.like_count
            FROM channels a
            INNER JOIN videos b 
            ON a.channel_id = b.channel_id
            ORDER BY b.like_count DESC
            LIMIT 10;
            """,
            """6. What is the total number of likes and dislikes for each video, and what are their corresponding
             video names?""": """
            SELECT a.channel_name, b.title, like_count AS total_likes, b.dislike_count AS total_dislikes
            FROM channels a
            INNER JOIN videos b ON a.channel_id = b.channel_id
            ORDER BY b.like_count DESC;
            """,
            "7. What is the total number of views for each channel, and what are their corresponding channel names?":
            """
            SELECT a.channel_name, SUM(b.view_count) AS total_views
            FROM channels a
            INNER JOIN videos b ON a.channel_id = b.channel_id
            GROUP BY a.channel_name;
            """,
            "8. What are the names of all the channels that have published videos in the year 2022?": """
            SELECT a.channel_name, b. title ,DATE(b.published_at)
            FROM channels a
            INNER JOIN videos b ON a.channel_id = b.channel_id
            WHERE YEAR(published_at) = 2022;
            """,
            """9. What is the average duration of all videos in each channel, and what are their corresponding channel
             names?""": """
            SELECT a.channel_name, AVG(TIME_TO_SEC(duration)) AS avg_duration_seconds
            FROM channels a
            INNER JOIN videos b ON a.channel_id = b.channel_id
            GROUP BY a.channel_name;
            """,
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT a.channel_name, b.title, b.comment_count
            FROM channels a
            INNER JOIN videos b ON a.channel_id = b.channel_id
            ORDER BY comment_count DESC
            LIMIT 10;
            """,
            "11. To view CHANNELS TABLE": """
            SELECT * FROM channels;
            """,
            "12. To view VIDEOS TABLE": """
            SELECT * FROM videos;
            """,
            "13. To view PLAYLISTS TABLE": """
            SELECT * FROM playlists;
            """,
            "14. To view COMMENTS TABLE": """
            SELECT * FROM comments;
            """,
        }

        # Allow user to select queries
        selected_queries = st.multiselect("Select queries:", list(queries.keys()))

        try:
            self.mysql_connection = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )

            self.mysql_cursor = self.mysql_connection.cursor()

            # Execute selected queries and display results
            for query_title in selected_queries:
                query = queries[query_title]
                self.mysql_cursor.execute(query)
                result = self.mysql_cursor.fetchall()

                if result:
                    st.subheader(query_title)
                    df = pd.DataFrame(result, columns=[i[0] for i in self.mysql_cursor.description])
                    st.dataframe(df)
                else:
                    st.write("No results found for query:", query_title)

        except mysql.connector.Error as error:
            st.error(f"Error occurred: {error}")

        finally:
            if 'self.mysql_connection' in locals() and self.mysql_connection.is_connected():
                self.mysql_cursor.close()
                self.mysql_connection.close()

    def main(analyzer):

        # Set page configuration
        st.set_page_config(
            page_title="YouTube Analytics Dashboard",
            page_icon="📊",
            layout="wide"
        )

        # Background YouTube logo image
        st.image("https://www.gstatic.com/youtube/img/branding/youtubelogo/svg/youtubelogo.svg", use_column_width=True)

        st.title("YouTube Analytics Dashboard")
        st.sidebar.title("YouTube API Connection")

        # User inputs
        api_key = st.sidebar.text_input("Enter your YouTube Data API key:",
                                        placeholder="Enter your YouTube Data API key here", type="password")

        st.sidebar.title("MongoDB Atlas Database Connection")

        mongodb_uri = st.sidebar.text_input("Enter your MongoDB Atlas URI:",
                                            placeholder="Enter your MongoDB Atlas URI here", type="password")
        mongodb_db_name = st.sidebar.text_input("Enter your MongoDB database name:",
                                                placeholder="Enter your MongoDB database name here")

        st.sidebar.title("AWS MySQL Database Connection")

        mysql_host = st.sidebar.text_input("Enter your AWS/MySQL host:",
                                           placeholder="Enter your AWS/MySQL host here")
        mysql_user = st.sidebar.text_input("Enter your AWS/MySQL username:",
                                           placeholder="Enter your AWS/MySQL username here")
        mysql_password = st.sidebar.text_input("Enter your AWS/MySQL password:",
                                               placeholder="Enter your AWS/MySQL password here", type="password")
        mysql_database = st.sidebar.text_input("Enter your AWS/MySQL database name:",
                                               placeholder="Enter your AWS/MySQL database name here")

        num_channels = st.number_input("Enter the number of channels to analyze:", min_value=1, max_value=10,
                                       step=1)

        # Input fields for channel names
        channel_names = []
        for i in range(num_channels):
            channel_name = st.text_input(f"Enter channel name {i + 1}:")
            channel_names.append(channel_name)

        if st.button("Analyze Channels"):

            # Authenticate with API key
            analyzer.authenticate(api_key)

            output = analyzer.analyze_channels(channel_names)

            # Insert data into MongoDB Atlas
            analyzer.insert_data_to_mongodb(output, mongodb_uri, mongodb_db_name)

            # Create tables in MySQL
            analyzer.create_mysql_tables(mysql_host, mysql_user, mysql_password, mysql_database)

            # Import data from MongoDB to MySQL
            analyzer.import_data_to_mysql(mongodb_uri, mongodb_db_name, mysql_host, mysql_user, mysql_password,
                                          mysql_database)

            # Select queries section
        st.sidebar.title("Select Queries")
        analyzer.select_and_execute_queries(mysql_host, mysql_user, mysql_password, mysql_database)


if __name__ == "__main__":
    # Create an instance of YouTubeChannelAnalyzer
    analyzer = YouTubeChannelAnalyzer()

    # Call the main function
    analyzer.main()
