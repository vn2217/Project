# -- APAN 5310: SQL & RELATIONAL DATABASES

#Create a conection
import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
conn_url = 'postgresql://postgres:proton20@localhost/checkpoint4'
engine = create_engine(conn_url)
con = engine.connect()

#uploading dataset1 (make sure all the datasets are saved as CSV UTF-8)
df1 = pd.read_csv (r'C:\CU\SQL II\USvideos.csv')
df2 = pd.read_csv (r'C:\CU\SQL II\RUvideos.csv')
df3 = pd.read_csv (r'C:\CU\SQL II\MXvideos.csv')
df4 = pd.read_csv (r'C:\CU\SQL II\KRvideos.csv')
df5 = pd.read_csv (r'C:\CU\SQL II\JPvideos.csv')
df6 = pd.read_csv (r'C:\CU\SQL II\INvideos.csv')
df7 = pd.read_csv (r'C:\CU\SQL II\GBvideos.csv')
df8 = pd.read_csv (r'C:\CU\SQL II\FRvideos.csv')
df9 = pd.read_csv (r'C:\CU\SQL II\DEvideos.csv')
df10 = pd.read_csv (r'C:\CU\SQL II\CAvideos.csv')

#create new column to designate country source
df1['Source'] = 'US'
df2['Source'] = 'RU'
df3['Source'] = 'MX'
df4['Source'] = 'KR'
df5['Source'] = 'JP'
df6['Source'] = 'IN'
df7['Source'] = 'GB'
df8['Source'] = 'FR'
df9['Source'] = 'DE'
df10['Source'] = 'CA'

#combining all the tables together
frames = [df1,df2,df3,df4,df5,df6,df7,df8,df9,df10]
df11 = pd.concat(frames)
#checking for missing values
df11.isnull().sum()
#creating a unique id for every entry in the dataset
df11['entry_id'] = df11.index if df11.index.is_monotonic_increasing else range(len(df11))

df11.to_sql('DM1', con)



#creating tables to match our 3NF layout
df12 = df11.copy()
del df12['comment_count']
del df12['thumbnail_link']
del df12['comments_disabled']
del df12['ratings_disabled']
del df12['video_error_or_removed']
del df12['description']
del df12['Source']
df12.to_sql('Trending Youtube Video Statistics', con)

#creating tables to match our 3NF layout
df13 = df11.copy()
del df13['trending_date']
del df13['title']
del df13['category_id']
del df13['publish_time']
del df13['tags']
del df13['views']
del df13['dislikes']
del df13['comment_count']
del df13['thumbnail_link']
del df13['comments_disabled']
del df13['ratings_disabled']
del df13['video_error_or_removed']
del df13['description']
del df13['Source']
df13.to_sql('Likes Table', con)

#creating tables to match our 3NF layout
df14 = df11.copy()
del df14['trending_date']
del df14['title']
del df14['category_id']
del df14['publish_time']
del df14['tags']
del df14['views']
del df14['likes']
del df14['comment_count']
del df14['thumbnail_link']
del df14['comments_disabled']
del df14['ratings_disabled']
del df14['video_error_or_removed']
del df14['description']
del df14['Source']
df14.to_sql('Dislikes Table', con)

#creating tables to match our 3NF layout
df15 = df11.copy()
del df15['trending_date']
del df15['title']
del df15['category_id']
del df15['publish_time']
del df15['tags']
del df15['dislikes']
del df15['likes']
del df15['comment_count']
del df15['thumbnail_link']
del df15['comments_disabled']
del df15['ratings_disabled']
del df15['video_error_or_removed']
del df15['description']
del df15['Source']
df15.to_sql('Views Table', con)

#creating tables to match our 3NF layout
df16 = df11.copy()
del df16['trending_date']
del df16['title']
del df16['category_id']
del df16['publish_time']
del df16['views']
del df16['dislikes']
del df16['likes']
del df16['comment_count']
del df16['thumbnail_link']
del df16['comments_disabled']
del df16['ratings_disabled']
del df16['video_error_or_removed']
del df16['description']
del df16['Source']
df16.to_sql('Tag Table', con)

#creating tables to match our 3NF layout
df17 = df11.copy()
del df17['trending_date']
del df17['title']
del df17['category_id']
del df17['publish_time']
del df17['views']
del df17['dislikes']
del df17['likes']
del df17['comment_count']
del df17['thumbnail_link']
del df17['comments_disabled']
del df17['ratings_disabled']
del df17['video_error_or_removed']
del df17['description']
df17.to_sql('Country Table', con)

#uploading dataset2 (make sure all the datasets are saved as CSV UTF-8)
df18 = pd.read_csv (r'C:\CU\SQL II\Dataset 2.csv')
df18['entry_id'] = df18.index if df18.index.is_monotonic_increasing else range(len(df18))
#checking for missing values
df18.isnull().sum()
#creating tables to match our 3NF layout
df19 = df18.copy()
del df19['comments_disabled']
del df19['ratings_disabled']
del df19['tag_appeared_in_title_count']
del df19['tag_appeared_in_title']
del df19['title']
del df19['tags']
del df19['description']
del df19['trend_day_count']
del df19['trend.publish.diff']
del df19['trend_tag_highest']
del df19['trend_tag_total']
del df19['tags_count']
del df19['subscriber']
df19.to_sql('Dataset 2 Table', con)

#creating tables to match our 3NF layout
df20 = df18.copy()
del df20['last_trending_date']
del df20['publish_date']
del df20['publish_hour']
del df20['category_id']
del df20['channel_title']
del df20['views']
del df20['likes']
del df20['dislikes']
del df20['comment_count']
del df20['comments_disabled']
del df20['ratings_disabled']
del df20['tag_appeared_in_title_count']
del df20['tag_appeared_in_title']
del df20['title']
del df20['trend_day_count']
del df20['trend.publish.diff']
del df20['trend_tag_highest']
del df20['trend_tag_total']
del df20['tags_count']
del df20['subscriber']
df20.to_sql('Dataset 2 Descrption', con)

df21 = df18.copy()
del df21['last_trending_date']
del df21['publish_date']
del df21['publish_hour']
del df21['description']
del df21['tags']
del df21['views']
del df21['likes']
del df21['dislikes']
del df21['comment_count']
del df21['comments_disabled']
del df21['ratings_disabled']
del df21['tag_appeared_in_title_count']
del df21['tag_appeared_in_title']
del df21['title']
del df21['trend_day_count']
del df21['trend.publish.diff']
del df21['trend_tag_highest']
del df21['trend_tag_total']
del df21['tags_count']
del df21['subscriber']
df21.to_sql('Dataset 2 Channel name', con)

#uploading dataset3 (make sure all the datasets are saved as CSV UTF-8)
df22 = pd.read_json(r'C:\CU\SQL II\YouTubeDataset_withChannelElapsed.json')
df22['entry_id'] = df22.index if df22.index.is_monotonic_increasing else range(len(df22))
Data = 'Projects'
#checking for missing values
df22.isnull().sum()
df22.to_sql(Data, con)

df26 = df22.copy()
del df26['channelId']
del df26['videoCategoryId']
del df26['channelViewCount']
del df26['likes/subscriber']
del df26['videoCount']
del df26['subscriberCount']
del df26['videoId']
del df26['dislikes/views']
del df26['channelelapsedtime']
del df26['comments/subscriber']
del df26['likes/views']
del df26['channelCommentCount']
del df26['videoViewCount']
del df26['likes/dislikes']
del df26['totvideos/videocount']
del df26['videoLikeCount']
del df26['videoDislikeCount']
del df26['dislikes/subscriber']
del df26['totviews/totsubs']
del df26['views/elapsedtime']
del df26['videoPublished']
del df26['VideoCommentCount']
df26.to_sql('Dataset 3 Project Table 2', con)

#uploading dataset5 (make sure all the datasets are saved as CSV UTF-8)
df23 = pd.read_csv (r'C:\CU\SQL II\Dataset 5.csv')
df23['entry_id'] = df23.index if df23.index.is_monotonic_increasing else range(len(df23))
Data = 'Artists'
#checking for missing values
df23.isnull().sum()
df23.to_sql(Data, con)

#uploading dataset4 (make sure all the datasets are saved as CSV UTF-8)
df24 = pd.read_csv (r'C:\CU\SQL II\Dataset 4.csv')
df24['entry_id'] = df24.index if df24.index.is_monotonic_increasing else range(len(df24))
Data = 'Dataset 4 User Table'
#checking for missing values
df24.isnull().sum()
df24.to_sql(Data, con)

df25 = df24.copy()
del df25['Rank']
del df25['Grade']
del df25['Uploads']
del df25['Views']
df25.to_sql('Dataset 4 User Table 2', con)

#15 analytics
#shows the total # of views across all the countries
View1 = df11.groupby(['Source'])["views"].sum()
View1

#shows the top 5 channels with most views
View2 = df11.groupby(['channel_title'])["views"].sum()
View2.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most likes
View3 = df11.groupby(['channel_title'])["likes"].sum()
View3.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most dislikes
View4 = df11.groupby(['channel_title'])["dislikes"].sum()
View4.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most views
View5 = df11.groupby(['title'])["views"].sum()
View5.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most likes
View6 = df11.groupby(['title'])["likes"].sum()
View6.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most dislikes
View7 = df11.groupby(['title'])["dislikes"].sum()
View7.sort_values(ascending=False).iloc[0:4]

#shows top 5 channels with the most views from the second dataset
View8 = df19.groupby(['channel_title'])["views"].sum()
View8.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most likes from the second dataset
View9 = df19.groupby(['channel_title'])["likes"].sum()
View9.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most dislikes from the second dataset
View10 = df19.groupby(['channel_title'])["dislikes"].sum()
View10.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most dislikes from the third dataset
View11 = df22.groupby(['channelId'])["channelViewCount"].sum()
View11.sort_values(ascending=False).iloc[0:4]

#shows the top 5 channels with most dislikes from the third dataset
View12 = df22.groupby(['channelId'])["channelViewCount"].sum()
View12.sort_values(ascending=False).iloc[0:4]

#shows the top 5 users with most views from the 4th dataset
View13 = df24.groupby(['Username'])["Views"].sum()
View13.sort_values(ascending=False).iloc[0:4]

#shows the top 5 artists with most views from the 5th dataset
View14 = df23.groupby(['Artist'])["Total"].sum()
View14.sort_values(ascending=False).iloc[0:4]

#shows the top 5 artists with most 100M videos from the 5th dataset
View15 = df23.groupby(['Artist'])["100M"].sum()
View15.sort_values(ascending=False).iloc[0:4]