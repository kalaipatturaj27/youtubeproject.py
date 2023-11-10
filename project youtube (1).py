#!/usr/bin/env python
# coding: utf-8

# In[1]:

# import tools

from googleapiclient.discovery import build  #built function used to create a client object ,Simplifies Authentication
import pandas as pd
import psycopg2
import pymongo
import streamlit as st


# In[2]:
#api key get

# youtube = googleapiclient.discovery.build(
        #api_service_name, api_version, credentials=credentials)
def api_connect():
    api_key ="AIzaSyAc3IBseELT5opqn72eDydbjPu6-oHddsA"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey = api_key) #build function to create a client for the Google Drive API
    
    return youtube

youtube =api_connect()


# In[3]:


#to get chanel information


def channel_info(channel_id):

    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    for i in response['items']:   # we dont need to put index.bcz each value in list assigned in i
        data = dict(channel_name = i["snippet"]['title'],
                   channel_id =i['id'],
                   subscriper_count =i['statistics']['subscriberCount'],
                   views = i['statistics']['viewCount'],
                    videos_count = i["statistics"]["videoCount"],
                    channel_description=i['snippet']['description'],
                    playlist_id = i['contentDetails']['relatedPlaylists']['uploads'])
    return data



# In[4]:


# to get videosid details
def videoid_details(channel_id):
    videos_id = []
    response = youtube.channels().list(id = channel_id,
                    part="contentDetails").execute()
    playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
                   
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
                    part="snippet",
                    playlistId =playlist_Id ,
                    maxResults = 50,
                    pageToken = next_page_token).execute()

        for i in range(len(response1['items'])):
            videos_id.append(response1['items'][i]['snippet']['resourceId']['videoId'])

        next_page_token =response1. get('nextPageToken') 

        if next_page_token is None:
            break
            
    return videos_id


# In[5]:


#get video details:

def video_details(video_Id):
    video_data =[]
    for video_id in video_Id:
        response =  youtube.videos().list(part="snippet,contentDetails,statistics",
                                          id=video_id).execute()
        for item in response['items']:
            data = dict( channel_name = item['snippet']['channelTitle'],
                        channel_Id = item['snippet']['channelId'],
                        video_id =item['id'],
                        video_title = item['snippet']['title'],
                        Tags =item['snippet'] .get('tags'),
                        Thumbnails =  item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet']. get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        comment_count = item['statistics'].get('commentCount'),
                        Favorite_count = item['statistics']['favoriteCount'],
                        definition  = item['contentDetails']['definition'],
                        Caption_status = item['contentDetails']['caption']
                       )
            video_data.append(data)
    return video_data
   


# In[6]:


#get comment details
def comment_info(video_Id):
    comment_data = []
    try:
        for video_id in video_Id:
            response = youtube.commentThreads().list(part = "snippet",
                                                     videoId =video_id,
                                                     maxResults=50).execute()
            for item in response['items']:
                data = dict(
                    comment_id = item['snippet']['topLevelComment']['id'],
                    video_id = item['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)
    except:
        pass
    
    return comment_data


# In[7]:


#get playlist info
def playlist_info(channel_id):
    next_page_token = None
    playlist_data =[]
    while True:
        response = youtube.playlists().list(
            part = "snippet,contentDetails",
            channelId =channel_id,
            maxResults = 50,
             pageToken =next_page_token).execute()


        for item in response['items']:
            data = dict(
                 Playlist_Id =item['id'],
                Title =item['snippet']['title'],
                channel_id = item['snippet']['channelId'],
                channel_name = item['snippet']['channelTitle'],
                Published_at = item['snippet']['publishedAt'],
                Video_count = item['contentDetails']['itemCount'])

            playlist_data.append(data)

        next_page_token =response .get('nextPageToken')
        if next_page_token is None:
            break
    return playlist_data



# In[8]:

# creat mongo db
#upload in mongodb
mongodb_url = "mongodb://localhost:27017/"
db_name = "youtube"
client = pymongo.MongoClient(mongodb_url)
db = client[db_name]
collection_name = "youtube_collection"
collection = db[collection_name]


# In[9]:


def channel_details(channel_id):
    ch_details =channel_info(channel_id)
    vid_details =videoid_details(channel_id)
    vi_details=video_details(vid_details)
    cmd_details=comment_info(vid_details)
    pl_details =playlist_info(channel_id)
    
    
    
    collection.insert_one({'channel_information':ch_details,"playlist_information": pl_details,
                          "video_information": vi_details, "comment_information":cmd_details})
    return "upload successfully"
        
    


# In[10]:


#'Science With Sam : 'UChGd9JY4yMegY6PxqpBjpRA'
#infobells - Tamil :"UCHcn4Ux-sO9nzNu7rDsoQLg"
#"ChuChuTV Tamil" :"UClPHwLcsE9zQcIqNRc7VLHw"


# In[11]:


## tables creation for channels,video,playlist 
def channels_table():
    mydb = psycopg2.connect(host = "localhost",
                            user ="postgres",
                            password ="kalai",
                            database = "youtube",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query=''' drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:

        create_qyery ='''create table if not exists channels(channel_name varchar(100),
                                                            channel_id varchar(80) primary key,
                                                            subscriper_count bigint,
                                                            views bigint,
                                                            videos_count int,
                                                            channel_description text,
                                                            playlist_id varchar(80))'''
        cursor.execute(create_qyery)

        mydb.commit()
    except:
        st.write("channels table already created")


    ch_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df= pd.DataFrame(ch_list)

    for index,row in df.iterrows():
        insert_qyery ='''insert into channels(channel_name ,
                                                            channel_id ,
                                                            subscriper_count ,
                                                            views ,
                                                            videos_count ,
                                                            channel_description ,
                                                            playlist_id )

                                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['channel_name'],
                row['channel_id'],
                row['subscriper_count'],
                row['views'],
                row['videos_count'],
                row['channel_description'],
                row['playlist_id'])

        try:
            cursor.execute(insert_qyery,values)
            mydb.commit()
        except:
            st.write("channels values already inserted")


# In[12]:


def playlists_table():
    mydb = psycopg2.connect(host = "localhost",
                            user ="postgres",
                            password ="kalai",
                            database = "youtube",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query=''' drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()
    


    try:
        create_query ='''create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                             Title text,
                                                            channel_id varchar(100),
                                                            channel_name varchar(100),
                                                            Published_at timestamp,
                                                            Video_count int)'''
        cursor.execute(create_query)

        mydb.commit()
    except:
        st.write("Playlists Table alredy created")


    play_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]


    for play_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])
    df1= pd.DataFrame(play_data["playlist_information"])

    for index,row in df1.iterrows():
        insert_query ='''insert into playlists(Playlist_Id ,
                                                Title,
                                                channel_id ,
                                                channel_name,
                                                Published_at,
                                                Video_count)

                                                values(%s,%s,%s,%s,%s,%s)'''

        values=(row['Playlist_Id'],
                row['Title'],
                row['channel_id'],
                row['channel_name'],
                row['Published_at'],
                row['Video_count'])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
             st.write("Playlists values are already inserted")





# In[13]:


def videos_table():

    mydb = psycopg2.connect(host = "localhost",
                            user ="postgres",
                            password ="kalai",
                            database = "youtube",
                            port = "5432")
    cursor = mydb.cursor()

    drop_query=''' drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_qyery ='''create table if not exists videos( channel_name varchar(100),
                                                            channel_Id varchar(100),
                                                            video_id varchar(30) primary key,
                                                            video_title varchar(150),
                                                            Tags text,
                                                            Thumbnails varchar(200),
                                                            Description text,
                                                            Published_Date timestamp,
                                                            Duration interval,
                                                            Views bigint,
                                                            Likes bigint,
                                                            comment_count int,
                                                            Favorite_count int,
                                                            definition  varchar(10),
                                                            Caption_status varchar(50))'''
        cursor.execute(create_qyery)

        mydb.commit()
    except:
        st.write("videos tables already created")


    video_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for video_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    df2= pd.DataFrame(video_data["video_information"])

    for index,row in df2.iterrows():
        insert_query ='''insert into videos(channel_name,
                                            channel_Id,
                                            video_id,
                                            video_title,
                                            Tags,
                                            Thumbnails,
                                            Description,
                                            Published_Date,
                                            Duration,
                                            Views,
                                            Likes,
                                            comment_count,
                                            Favorite_count,
                                            definition,
                                            Caption_status
                                            )
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''





        values=(row['channel_name'],
                row['channel_Id'],
                row['video_id'],
                row['video_title'],
                row['Tags'],
                row['Thumbnails'],
                row['Description'],
                row['Published_Date'],
                row['Duration'],
                row['Views'],
                row['Likes'],
                row['comment_count'],
                row['Favorite_count'],
                row['definition'],
                row['Caption_status'])


        try:
            
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            
            st.write("vides values already inserted")



# In[14]:


def comments_table():

    mydb = psycopg2.connect(host = "localhost",
                                user ="postgres",
                                password ="kalai",
                                database = "youtube",
                                port = "5432")
    cursor = mydb.cursor()

    drop_query=''' drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    try:
        create_qyery ='''create table if not exists comments( comment_id varchar(100) primary key,
                                                                video_id varchar(100),
                                                                comment_text text,
                                                                comment_author varchar(100),
                                                                comment_published timestamp )'''
        cursor.execute(create_qyery)

        mydb.commit()
    except:
        st.write("comments tables already inserted")

    comment_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for comment_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    df3= pd.DataFrame(comment_data["comment_information"])

    for index,row in df3.iterrows():
        insert_query ='''insert into comments(comment_id,
                                             video_id,
                                            comment_text,
                                            comment_author,
                                            comment_published) 
                                            values(%s,%s,%s,%s,%s)'''





        values=(row['comment_id'],
                row['video_id'],
                row['comment_text'],
                row['comment_author'],
                row['comment_published'])


        try:
            cursor.execute(insert_query,values)
            mydb.commit()  
            
        except:
            
            st.write("comments values already inserted")




# In[15]:


def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "tables created successfully"
    


# In[16]:


def show_channels_table():

    ch_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for ch_data in collection.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    channels_tables= st.dataframe(ch_list)
    return channels_tables


# In[17]:


def show_playlist_table():
    play_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]


    for play_data in collection.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(play_data["playlist_information"])):
            play_list.append(play_data["playlist_information"][i])
    play_table= st.dataframe(play_data["playlist_information"])
    return play_table


# In[18]:


def show_video_table(): 
    
    video_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for video_data in collection.find({},{"_id":0,"video_information":1}):
        for i in range(len(video_data["video_information"])):
            video_list.append(video_data["video_information"][i])
    video_table= st.dataframe(video_data["video_information"])
    return video_table


# In[19]:


def show_comment_table():

    comment_list=[]
    db = client["youtube"]
    collection = db["youtube_collection"]
    for comment_data in collection.find({},{"_id":0,"comment_information":1}):
        for i in range(len(comment_data["comment_information"])):
            comment_list.append(comment_data["comment_information"][i])
    comment_table= st.dataframe(comment_data["comment_information"])
    return comment_table



#streamlit part.

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("SKILL TAKE AWAY")
    st.caption('Python scripting')
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption(" Data Managment using MongoDB and SQL")
    
channel_id = st.text_input("Enter the Channel id")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]


# In[21]:


if st.button("Collect and Store data"):
    for channel in channels:
        ch_ids = []
        db = client["youtube"]
        collection = db["youtube_collection"]
        for ch_data in collection.find({},{"_id":0,"channel_information":1}):
            ch_ids.append(ch_data["channel_information"]["channel_id"])
        if channel_id in ch_ids:
            st.success("Channel details of the given channel id: " + channel + " already exists")
        else:
            output = channel_details(channel_id)
            st.success(output)


# sql connection in streamlit


if st.button("Migrate to SQL"):
    display = tables()
    st.success(display)
    
show_table = st.radio("SELECT THE TABLE FOR VIEW",(":green[channels]",":orange[playlists]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    show_channels_table()
elif show_table == ":orange[playlists]":
    show_playlist_table()
elif show_table ==":red[videos]":
    show_video_table()
elif show_table == ":blue[comments]":
    show_comment_table()


# In[24]:


#SQL connection
mydb = psycopg2.connect(host="localhost",
            user="postgres",
            password="kalai",
            database= "youtube",
            port = "5432"
            )
cursor = mydb.cursor()
    
question = st.selectbox('Please Select Your Question',
                        ('1. All the videos and the Channel Name',
                         '2. Channels with most number of videos',
                         '3. 10 most viewed videos',
                         '4. Comments in each video',
                         '5. Videos with highest likes',
                         '6. likes of all videos',
                         '7. views of each channel',
                         '8. videos published in the year 2022',
                         '9. average duration of all videos in each channel',
                         '10. videos with highest number of comments'))


# In[25]:


if question == '1. All the videos and the Channel Name':
    query1 = "select video_title as Video Title,channel_name as Channel Name from videos;"
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    st.write(pd.DataFrame(t1, columns=["Video Title","Channel Name"]))

elif question == '2. Channels with most number of videos':
    query2 = "select channel_name as Channel Name,videos_count as NO_Videos from channels order by videos_count desc;"
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    st.write(pd.DataFrame(t2, columns=["Channel Name","No Of Videos"]))

elif question == '3. 10 most viewed videos':
    query3 = '''select views as views , channel_Name as ChannelName,video_title as VideoTitle from videos 
                        where views is not null order by views desc limit 10;'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    st.write(pd.DataFrame(t3, columns = ["views","channel Name","video title"]))

elif question == '4. Comments in each video':
    query4 = "select comment_count as No_comments ,video_title as VideoTitle from videos where comment_count is not null;"
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    st.write(pd.DataFrame(t4, columns=["No Of Comments", "Video Title"]))

elif question == '5. Videos with highest likes':
    query5 = '''select video_title as VideoTitle, channel_name as ChannelName, likes as LikesCount from videos 
                       where likes is not null order by likes desc;'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    st.write(pd.DataFrame(t5, columns=["video Title","channel Name","like count"]))

elif question == '6. likes of all videos':
    query6 = '''select likes as likeCount,video_title as VideoTitle from videos;'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["like count","video title"]))

elif question == '7. views of each channel':
    query7 = "select channel_name as ChannelName, views as Channelviews from channels;"
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["channel name","total views"]))

elif question == '8. videos published in the year 2022':
    query8 = '''select video_title as Video_Title, published_date as VideoRelease, channel_name as ChannelName from videos 
                where extract(year from published_date) = 2022;'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8,columns=["Name", "Video Publised On", "ChannelName"]))

elif question == '9. average duration of all videos in each channel':
    query9 =  "SELECT channel_name as ChannelName, AVG(duration) as average_duration from videos GROUP BY channel_name;"
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    t9 = pd.DataFrame(t9, columns=['ChannelTitle', 'Average Duration'])
    T9=[]
    for index, row in t9.iterrows():
        channel_title = row['ChannelTitle']
        average_duration = row['Average Duration']
        average_duration_str = str(average_duration)
        T9.append({"Channel Title": channel_title ,  "Average Duration": average_duration_str})
    st.write(pd.DataFrame(T9))

elif question == '10. videos with highest number of comments':
    query10 = '''select video_title as VideoTitle, channel_name as ChannelName, comment_count as Comments from videos 
                       where comment_count is not null order by comment_count desc;'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=['Video Title', 'Channel Name', 'NO Of Comments']))


# In[ ]:





# In[ ]:





# In[ ]:




