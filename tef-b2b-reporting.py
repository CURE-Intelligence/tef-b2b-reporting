# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Titel: Automated data collection for B2B reporting
# Project: B2B SoME Reporting
# Client: Telefónica 
# Author: Artur Galiev 
# Date: 11.01.2023 
# Last modified: Artur Galiev (11.01.2023) 
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

# DESCRIPTION: 
# The script is used to create a database for the B2B SoMe report based on Talkwalker data
# Data is collected dynamically via embedded widgets from Talkwalker
# 
# The script was programmed on Python 3.9.23 and is designed to be executed on a local pc

'''
Necessary Talkwalker Channels:

Facebook:
    - o2businessde
    - telekomGK
    - VodafoneBusiness
    - deutscheTelekom (x)
    - vodafoneDE (x)

Instagram:
    - o2businessde
    - vodafonebusinessde
    - deutschetelekom (x)
    - vodafone_de (x)

Twitter:
    - o2business
    - TelekomGK
    - vodafone_b2b
    - telefonica_de (x)
    - deutschetelekom (x)
    - vodafone_de (x)

YouTube:
    - Telefonica Deutschland (x)
    - telekombusiness
    - vodafonedeutschland (x)
    - deutscheTelekomAG (x)
'''

import pandas as pd
import numpy as np
import datetime
from dateutil.relativedelta import relativedelta
import xlwings as xw

#pylint: disable=undefined-variable

# %% [1] Wachstum Social Media Kanäle

# Erstelle DataFrame für Export
indexes=['o2Business','TelekomGK',"VodafoneB2B","Telefonica","DeutscheTelekom","Vodafone"]

today = datetime.date.today()
first_day = today.replace(day=1)
lastMonth = first_day - relativedelta(months=+1)
prevMonth = first_day - relativedelta(months=+2)
lastYear = lastMonth - relativedelta(years=+1)

lastMonth = lastMonth.strftime('%Y-%m-%d')
prevMonth = prevMonth.strftime('%Y-%m-%d')
lastYear = lastYear.strftime('%Y-%m-%d')

index_column_map = {
    'o2Business': ['@o2 Business Deutschland (Facebook)', 'o2businessde (Instagram Business Accounts)', '@o2business (Twitter)'],
    'TelekomGK': ['@Deutsche Telekom Business (Facebook)', '@TelekomGK (Twitter)','Telekom Business (YouTube)'],
    'VodafoneB2B': ['@Vodafone Business (Facebook)', 'vodafonebusinessde (Instagram Business Accounts)', '@Vodafone_B2B (Twitter)','Vodafone Business (YouTube)'],
    'Telefonica': ['@telefonica_de (Twitter)','Telefónica Germany (YouTube)'],
    'DeutscheTelekom': ['@deutschetelekom (Facebook)', 'deutschetelekom (Instagram Business Accounts)', '@deutschetelekom (Twitter)','Deutsche Telekom (YouTube)'],
    'Vodafone': ['@vodafoneDE (Facebook)', 'vodafone_de (Instagram Business Accounts)', '@vodafone_de (Twitter)','Vodafone Deutschland (YouTube)']}

#  Erstelle ein Dictionary mit den notwendigen embedded widgets
Follower_Growth = {
    "b2b_facebook_wachstum":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_Hp8JYCC4.csv",
    "b2b_instagram_wachstum":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_pK91AkMA.csv", 
    "b2b_twitter_wachstum":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_KRDNyNO1.csv",
    "b2b_YouTube_wachstum":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_LDyoaIUv.csv"
    }

dfs = []
for key, url in Follower_Growth.items():
    dfs.append(pd.read_csv(url))

b2b_facebook_wachstum, b2b_instagram_wachstum, b2b_twitter_wachstum, b2b_YouTube_wachstum = dfs

# Ändere das Format des Datums
dataframes = [b2b_facebook_wachstum, b2b_instagram_wachstum, b2b_twitter_wachstum, b2b_YouTube_wachstum]
for df in dataframes:
    df['Date'] = df['Date'].str.replace(' 00:00:00','',regex=True)
    df['Date'] = df['Date'].str.replace('.20','.2020',regex=True)
    df['Date'] = df['Date'].str.replace('.21','.2021',regex=True)
    df['Date'] = df['Date'].str.replace('.22','.2022',regex=True)
    df['Date'] = df['Date'].str.replace('.23','.2023',regex=True)
    df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y")
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    
    
follower_columns = ['Facebook_Follower_lastYear', 'Facebook_Follower_prevMonth', 'Facebook_Follower_lastMonth',
                'Instagram_Follower_lastYear', 'Instagram_Follower_prevMonth', 'Instagram_Follower_lastMonth',
                'Twitter_Follower_lastYear', 'Twitter_Follower_prevMonth', 'Twitter_Follower_lastMonth',
                'YouTube_Follower_lastYear', 'YouTube_Follower_prevMonth', 'YouTube_Follower_lastMonth']

df = pd.DataFrame(columns=[], index=indexes)


for index in indexes:
    if index not in index_column_map.keys():
        for date_column in follower_columns:
            df.loc[index, date_column] = np.nan
    else:
        columns = index_column_map[index]
        for date_column in follower_columns:
            column_name = np.nan
            if 'Facebook' in date_column:
                for column in columns:
                    if 'Facebook' in column:
                        df_temp = b2b_facebook_wachstum
                        column_name = column
                        break
            elif 'Instagram' in date_column:
                for column in columns:
                    if 'Instagram' in column:
                        df_temp = b2b_instagram_wachstum
                        column_name = column
                        break
            elif 'Twitter' in date_column:
                for column in columns:
                    if 'Twitter' in column:
                        df_temp = b2b_twitter_wachstum
                        column_name = column
                        break
            elif 'YouTube' in date_column:
                for column in columns:
                    if 'YouTube' in column:
                        df_temp = b2b_YouTube_wachstum
                        column_name = column
                        break
            else:
                df.loc[index,date_column] = np.nan
            
            if column_name in df_temp.columns:
                date_value = date_column.split('_')[-1]
                if date_value == 'lastYear':
                    date_value = lastYear
                elif date_value == 'prevMonth':
                    date_value = prevMonth
                elif date_value == 'lastMonth':
                    date_value = lastMonth
                df.loc[index,date_column] = int(df_temp.loc[df_temp["Date"] == date_value,column_name])
            else:
                df.loc[index,date_column] = np.nan

columns = ['Facebook_Follower', 'Instagram_Follower','Twitter_Follower','YouTube_Follower']


for col in columns:
    df[f'{col}_Growth_Year'] = np.where(df[f'{col}_lastYear'] == 0, "k. A.", round((df[f'{col}_lastMonth'] - df[f'{col}_lastYear']) / df[f'{col}_lastYear'] * 100, 2).astype(str) + "%")
    df[f'{col}_Growth_Year'] = np.where(df[f'{col}_Growth_Year'] != "k. A.", df[f'{col}_Growth_Year'].str.replace('.',',', regex=True), df[f'{col}_Growth_Year'])
    df[f'{col}_Growth_Year'] = df[f'{col}_Growth_Year'].replace("nan%", np.nan)
    
    df[f'{col}_Growth_Month'] = np.where(df[f'{col}_prevMonth'] == 0, "k. A.", round((df[f'{col}_lastMonth'] - df[f'{col}_prevMonth']) / df[f'{col}_prevMonth'] * 100, 2).astype(str) + "%")
    df[f'{col}_Growth_Month'] = np.where(df[f'{col}_Growth_Month'] != "k. A.", df[f'{col}_Growth_Month'].str.replace('.',',', regex=True), df[f'{col}_Growth_Month'])  
    df[f'{col}_Growth_Month'] = df[f'{col}_Growth_Month'].replace("nan%", np.nan)


follower_time_series = dfs[0]
for i in range(1, len(dfs)):
    follower_time_series = pd.merge(follower_time_series, dfs[i], on='Date')    
                                  
# %% [2] Acivity, Reach & Engagement
# %%% [2.1] Facebook 
Facebook_Activity = {
    "b2b_facebook_activity_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_ZyHR6vM1.csv",
    "b2b_facebook_activity_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_bQHbznNb.csv",
    "b2b_facebook_audience_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_PFzExeaF.csv",
    "b2b_facebook_audience_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_SBHyZZUr.csv",
    "b2b_facebook_reach_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_IXwxxXi8.csv",
    "b2b_facbeook_reach_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_ObfK9f7i.csv"
    }    

dfs = []
for key, url in Facebook_Activity.items():
    dfs.append(pd.read_csv(url))

# Die nachfolgende Zeile hat keinen Einfluss auf den Code und dient lediglich dazu, die Fehlermeldungen zu nicht definierten namen zu unterdrücken
b2b_facebook_activity_primary, b2b_facebook_activity_mixed, b2b_facebook_audience_primary, b2b_facebook_audience_mixed, b2b_facebook_reach_primary, b2b_facbeook_reach_mixed= dfs

b2b_facebook_activity = pd.concat([b2b_facebook_activity_primary,b2b_facebook_activity_mixed], ignore_index=True)
b2b_facebook_audience = pd.concat([b2b_facebook_audience_primary, b2b_facebook_audience_mixed], ignore_index=True)
b2b_facebook_reach = pd.concat([b2b_facebook_reach_primary, b2b_facbeook_reach_mixed], ignore_index=True)


# Definiere welche Spalten aus den jeweiligen Dataframes extrahiert werden sollen 
col_df_map = {
    "Facebook_Owner_Posts": b2b_facebook_activity,
    "Facebook_Audience_Comments": b2b_facebook_audience,
    "Facebook_Audience_Reactions": b2b_facebook_audience,
    "Facebook_Audience_Reach": b2b_facebook_audience,
    "Facebook_Engagement": b2b_facebook_reach,
    "Facebook_Reach" : b2b_facebook_reach,
    "Facebook_Results" : b2b_facebook_reach,
}

# columns = ["Facebook_Owner_Posts", "Facebook_Audience_Comments", "Facebook_Audience_Reactions", "Facebook_Audience_Reach", "Facebook_Engagement", "Facebook_Reach", "Facebook_Results"]
# for column in columns:
#     df[column] = 0
 
for index,cols in index_column_map.items():
    for col in cols:
        if "Facebook" in col:
            for key, value in col_df_map.items():
                if key == "Facebook_Owner_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Audience_Comments":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Audience Comments"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Audience_Reactions":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Facebook Reactions"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Audience_Reach":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Total Shares"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Engagement":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Engagement"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Reach":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Potential Reach"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Facebook_Results":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Results"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                
# %%% [2.2] Instagram
Instagram_Activity = {
    "b2b_insta_own_posts_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_ivvWloBS.csv",
    "b2b_insta_own_post_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_KB1UysmC.csv",
    "b2b_insta_likes_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_nU8unGNJ.csv",
    "b2b_insta_likes_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_fHnsGtQf.csv",
    "b2b_insta_comments_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_pMYSqKcy.csv",
    "b2b_insta_comments_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_QCTyUgW2.csv",
    "b2b_insta_engagementrate_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_utlCS5t6.csv",
    "b2b_insta_engagementrate_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_vCbo2PVu.csv",
    "b2b_insta_engagement_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_pORui9DY.csv",
    "b2b_insta_engagement_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_0wQVmxWn.csv"
    }

dfs = []
for key, url in Instagram_Activity.items():
    dfs.append(pd.read_csv(url))

# Die nachfolgende Zeile hat keinen Einfluss auf den Code und dient lediglich dazu, die Fehlermeldungen zu nicht definierten namen zu unterdrücken
b2b_insta_own_posts_primary,b2b_insta_own_post_mixed,b2b_insta_likes_primary,b2b_insta_likes_mixed,b2b_insta_comments_primary,b2b_insta_comments_mixed,b2b_insta_engagementrate_primary,b2b_insta_engagementrate_mixed,b2b_insta_engagement_primary,b2b_insta_engagement_mixed = dfs

b2b_insta_own_posts = pd.concat([b2b_insta_own_posts_primary,b2b_insta_own_post_mixed], ignore_index=True)
b2b_insta_likes = pd.concat([b2b_insta_likes_primary,b2b_insta_likes_mixed], ignore_index=True)
b2b_insta_comments = pd.concat([b2b_insta_comments_primary,b2b_insta_comments_mixed], ignore_index=True)
b2b_insta_engagementrate = pd.concat([b2b_insta_engagementrate_primary,b2b_insta_engagementrate_mixed], ignore_index=True)
b2b_insta_engagementrate["Results"] = b2b_insta_engagementrate["Results"].str.replace("%","")
b2b_insta_engagement = pd.concat([b2b_insta_engagement_primary,b2b_insta_engagement_mixed], ignore_index=True)

# Definiere welche Spalten aus den jeweiligen Dataframes extrahiert werden sollen 
col_df_map = {
    "Instagram_Owner_Posts": b2b_insta_own_posts,
    "Instagram_Audience_Likes": b2b_insta_likes,
    "Instagram_Audience_Comments": b2b_insta_comments,
    "Instagram_Engagement_Rate":b2b_insta_engagementrate,
    "Instagram_Engagement":b2b_insta_engagement
    }

# columns = ["Instagram_Owner_Posts", "Instagram_Audience_Likes", "Instagram_Audience_Comments", "Instagram_Engagement_Rate", "Instagram_Engagement"]
# for column in columns:
#     df[column] = 0

for index,cols in index_column_map.items():
    for col in cols:
        if "Instagram" in col:
            for key, value in col_df_map.items():
                if key == "Instagram_Owner_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Total Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Instagram_Audience_Likes":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Likes"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Instagram_Audience_Comments":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Comments"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Instagram_Engagement_Rate":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Results"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Instagram_Engagement":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Engagement"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0

# %%% [2.3] YouTube
YouTube_Activity = {
    "b2b_yt_own_posts_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_H27cgecQ.csv",
    "b2b_yt_own_posts_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_PPE4kcEv.csv",
    "b2b_yt_views_comments_likes_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_hOEPA8BW.csv",
    "b2b_yt_views_comments_likes_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_7aZWn1UF.csv",
    "b2b_yt_reach_engagement_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_FlasPKsd.csv" #Primary Channel ist hier bereits enthalten, da ansonsten 3 weitere Embedded Widgets erstell hätten werden müssen
    }

dfs = []
for key, url in YouTube_Activity.items():
    dfs.append(pd.read_csv(url))

# Die nachfolgende Zeile hat keinen Einfluss auf den Code und dient lediglich dazu, die Fehlermeldungen zu nicht definierten namen zu unterdrücken
b2b_yt_own_posts_primary,b2b_yt_own_posts_mixed,b2b_yt_views_comments_likes_primary,b2b_yt_views_comments_likes_mixed,b2b_yt_reach_engagement_mixed = dfs

b2b_yt_own_posts = pd.concat([b2b_yt_own_posts_primary,b2b_yt_own_posts_mixed], ignore_index=True)
b2b_yt_views_comments_likes = pd.concat([b2b_yt_views_comments_likes_primary,b2b_yt_views_comments_likes_mixed], ignore_index=True)

# Definiere welche Spalten aus den jeweiligen Dataframes extrahiert werden sollen 
col_df_map = {
    "YouTube_Owner_Posts": b2b_yt_own_posts,
    "YouTube_Views": b2b_yt_views_comments_likes,
    "YouTube_Audience_Comments": b2b_yt_views_comments_likes,
    "YouTube_Audience_Likes": b2b_yt_views_comments_likes,
    "YouTube_Audience_Dislikes": b2b_yt_views_comments_likes,
    "YouTube_Audience_Engagement": b2b_yt_reach_engagement_mixed,
    "YouTube_Audience_Reach": b2b_yt_reach_engagement_mixed,
    "YouTube_Audience_Results": b2b_yt_reach_engagement_mixed,
    }
    
# columns = ["Youube_Owner_Posts", "YouTube_Views", "YouTube_Audience_Comments", "YouTube_Audience_Likes", "YouTube_Audience_Dislikes", "YouTube_Audience_Engagement", "YouTube_Audience_Reach", "YouTube_Audience_Results"]
# for column in columns:
#     df[column] = 0

for index,cols in index_column_map.items():
    for col in cols:
        if "YouTube" in col:
            for key, value in col_df_map.items():
                if key == "YouTube_Owner_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Views":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Views"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Comments":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Audience Comments"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Likes":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Likes"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Dislikes":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Dislikes"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Engagement":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Engagement"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Reach":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Potential Reach"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "YouTube_Audience_Results":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Results"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0

# %%% [2.4] Twitter
Twitter_Activity = {
    "b2b_tw_own_posts_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_WCn12Gr0.csv",
    "b2b_tw_own_posts_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_LyRTanMv.csv",
    "b2b_tw_likes_replies_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_YsqrQJfd.csv",
    "b2b_tw_likes_replies_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_WUGulvdn.csv",
    "b2b_tw_engagement_reach_primary":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_Iz6xexCx.csv",
    "b2b_tw_engagement_reach_mixed":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_UEJRheSU.csv"
    }

dfs = []
for key, url in Twitter_Activity.items():
    dfs.append(pd.read_csv(url))

# Die nachfolgende Zeile hat keinen Einfluss auf den Code und dient lediglich dazu, die Fehlermeldungen zu nicht definierten namen zu unterdrücken
b2b_tw_own_posts_primary, b2b_tw_own_posts_mixed, b2b_tw_likes_replies_primary, b2b_tw_likes_replies_mixed, b2b_tw_engagement_reach_primary, b2b_tw_engagement_reach_mixed = dfs
b2b_tw_own_posts = pd.concat([b2b_tw_own_posts_primary,b2b_tw_own_posts_mixed], ignore_index=True)
b2b_tw_likes_replies = pd.concat([b2b_tw_likes_replies_primary,b2b_tw_likes_replies_mixed], ignore_index=True)
b2b_tw_engagement_reach = pd.concat([b2b_tw_engagement_reach_primary,b2b_tw_engagement_reach_mixed], ignore_index=True)
    
# Definiere welche Spalten aus den jeweiligen Dataframes extrahiert werden sollen 
col_df_map = {
    "Twitter_Owner_Posts": b2b_tw_own_posts,
    "Twitter_Audience_Likes": b2b_tw_likes_replies,
    "Twitter_Audience_Replies": b2b_tw_likes_replies,
    "Twitter_Audience_Retweets": b2b_tw_likes_replies,
    "Twitter_Audience_Engagement": b2b_tw_engagement_reach,
    "Twitter_Audience_Reach": b2b_tw_engagement_reach,
    "Twitter_Audience_Results": b2b_tw_engagement_reach,
    }
    
# columns = ["Twitter_Owner_Posts", "Twitter_Audience_Likes", "Twitter_Audience_Replies", "Twitter_Audience_Retweets", "Twitter_Audience_Engagement", "Twitter_Audience_Reach", "Twitter_Audience_Results"]
# for column in columns:
#     df[column] = 0

for index,cols in index_column_map.items():
    for col in cols:
        if "Twitter" in col:
            for key, value in col_df_map.items():
                if key == "Twitter_Owner_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Tweets"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Likes":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Likes"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Replies":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Replies"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Retweets":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Audience Retweets"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Engagement":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Engagement"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Reach":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Potential Reach"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
                elif key == "Twitter_Audience_Results":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Results"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0

# %% [3] Sentiment
    
Sentiment = {
    "b2b_fb_o2business_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_GRCMIMBN.csv",
    "b2b_fb_telekomgk_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_iDyKx2bZ.csv",
    "b2b_fb_vodafonebusiness_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_6ByyvfF9.csv",
    "b2b_fb_deutschetelekom_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_7xjQy3vn.csv",
    "b2b_fb_vodafonede_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_NoIenYnX.csv",
    
    "b2b_insta_o2business_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_yb8Gw8zF.csv",
    "b2b_insta_vodafonebusinessde_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_rUZw7ulU.csv",
    "b2b_insta_deutschetelekom_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_0zVnyKQQ.csv",
    "b2b_insta_vodafone_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_lCTRj8By.csv",
    
    "b2b_tw_o2business_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_Zle7UMux.csv",
    "b2b_tw_telekomgk_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_vGkBHL8T.csv",
    "b2b_tw_vodafoneb2b_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_WypVbOqj.csv",
    "b2b_tw_telefonicade_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_QmkzQ30l.csv",
    "b2b_tw_deutschetelekom_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_YAVZm080.csv",
    "b2b_tw_vodafonede_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_PHC2c6dX.csv",
    
    "b2b_yt_telefonicade_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_9n5gkOQI.csv",
    "b2b_yt_telekombusiness_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_SBHYCIMw.csv",
    "b2b_yt_vodafonede_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_Ud1eD8zl.csv",
    "b2b_yt_deutschetelekomag_x_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_sSgFxdV7.csv",
    
    "b2b_fb_o2cando_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_Yy48eFXT.csv",
    "b2b_insta_o2cando_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_lzibfwCr.csv",
    "b2b_tw_o2cando_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_nsISQUk5.csv",
    "b2b_yt_o2cando_sentiment":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_8re8oMkO.csv"
    }

dfs = []
# for key, url in Sentiment.items():
for key, url in Sentiment.items():
    dfs.append(pd.read_csv(url))

date_column = None
for i,temp_df in enumerate(dfs):
    if 'Date' in temp_df.columns:
        temp_df['Date'] = temp_df['Date'].apply(lambda x: datetime.datetime.strptime(x, "%d.%m.%y %H:%M:%S").strftime("%d.%m.%Y"))
        dfs[i] = dfs[i].set_index('Date')
        date_column = temp_df['Date']
    else:
        print("Dataframe at index ",i," does not contain a column named 'Date'.")
        temp_df['Date'] = date_column
        dfs[i] = dfs[i].set_index('Date')
    cols = ['Positive', 'Neutral', 'Negative']
    for col in cols:
        if col not in dfs[i].columns:
            dfs[i][col] = [0]*len(dfs[i])
    dfs[i] = dfs[i][cols] #sort the columns
    sums = dfs[i][cols].sum()
    dfs[i].loc['Gesamt'] = [sums[col] for col in cols]
    dfs[i].insert(0, 'placeholder', None)

(b2b_fb_o2business_sentiment, b2b_fb_telekomgk_sentiment, 
    b2b_fb_vodafonebusiness_sentiment, b2b_fb_deutschetelekom_x_sentiment, 
    b2b_fb_vodafonede_x_sentiment, b2b_insta_o2business_sentiment, 
    b2b_insta_vodafonebusinessde_sentiment, b2b_insta_deutschetelekom_x_sentiment, 
    b2b_insta_vodafone_x_sentiment, b2b_tw_o2business_sentiment, 
    b2b_tw_telekomgk_sentiment, b2b_tw_vodafoneb2b_sentiment, 
    b2b_tw_telefonicade_x_sentiment, b2b_tw_deutschetelekom_x_sentiment, 
    b2b_tw_vodafonede_x_sentiment,  b2b_yt_telefonicade_x_sentiment, 
    b2b_yt_telekombusiness_sentiment, b2b_yt_vodafonede_x_sentiment, 
    b2b_yt_deutschetelekomag_x_sentiment,
    b2b_fb_o2cando_sentiment, b2b_insta_o2cando_sentiment,
    b2b_tw_o2cando_sentiment, b2b_yt_o2cando_sentiment) = dfs
    

# %% [4] Total Posts Mixed Channels
Total_Posts = {
    "b2b_fb_mixed_total_posts":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_u8WdoNPk.csv",
    "b2b_insta_mixed_total_posts":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_G94BJWYT.csv",
    "b2b_tw_mixed_total_posts":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_4yqiTRvJ.csv",
    "b2b_yt_mixed_total_posts":"https://app.talkwalker.com/app/project/cfcc2e9b-1aa6-4571-b108-fba6a1d2e84a/cached/export_Telef%C3%B3nicaGermany_5RYSnquS.csv"
    }

dfs = []

for key, url in Total_Posts.items():
    dfs.append(pd.read_csv(url))
    
b2b_fb_mixed_total_posts, b2b_insta_mixed_total_posts, b2b_tw_mixed_total_posts, b2b_yt_mixed_total_posts = dfs

# Definiere welche Spalten aus den jeweiligen Dataframes extrahiert werden sollen 
col_df_map = {
    "Facebook_Mixed_Total_Posts": b2b_fb_mixed_total_posts,
    "Instagram_Mixed_Total_Posts": b2b_insta_mixed_total_posts,
    "Twitter_Mixed_Total_Posts": b2b_tw_mixed_total_posts,
    "YouTube_Mixed_Total_Posts": b2b_yt_mixed_total_posts
    }

for index,cols in index_column_map.items():
    for col in cols:
        if "Facebook" in col:
            for key, value in col_df_map.items():
                if key == "Facebook_Mixed_Total_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
        if "Instagram" in col:
            for key, value in col_df_map.items():
                if key == "Instagram_Mixed_Total_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Total Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0        
        if "Twitter" in col:
            for key, value in col_df_map.items():
                if key == "Twitter_Mixed_Total_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Tweets"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0
        if "YouTube" in col:
            for key, value in col_df_map.items():
                if key == "YouTube_Mixed_Total_Posts":
                    df.loc[index, key] = value.query(f"Labels=='{col}'")["Owner Posts"].values[0] if len(value.query(f"Labels=='{col}'"))>0 else 0

# %% [5] Create Export Files
date = datetime.datetime.strptime(lastMonth, "%Y-%m-%d")

# load workbook
app = xw.App(visible=True)
wb = xw.Book('V:/CURE/Operations/Clients/Telefónica/Data Science Projects/Wettbewerbsvergleich_B2B_Automatisierung/b2b_report_db.xlsx')  

# Insert into Excel
ws = wb.sheets['Database']
ws.range('A1').options(index=True).value = df    
ws.range('B10').value = date.strftime("%B")

ws = wb.sheets['Follower_Growth']
ws.range('A1').options(index=False).value = follower_time_series
    
ws = wb.sheets['Sentiment']
ws.range('A3').value = b2b_fb_o2business_sentiment
ws.range('G3').value = b2b_fb_telekomgk_sentiment
ws.range('M3').value = b2b_fb_vodafonebusiness_sentiment
ws.range('Y3').value = b2b_fb_deutschetelekom_x_sentiment
ws.range('AE3').value = b2b_fb_vodafonede_x_sentiment
ws.range('AK3').value = b2b_fb_o2cando_sentiment

ws.range('A21').value = b2b_insta_o2business_sentiment
ws.range('M21').value = b2b_insta_vodafonebusinessde_sentiment
ws.range('Y21').value = b2b_insta_deutschetelekom_x_sentiment
ws.range('AE21').value = b2b_insta_vodafone_x_sentiment
ws.range('AK21').value = b2b_insta_o2cando_sentiment

ws.range('A39').value = b2b_tw_o2business_sentiment
ws.range('G39').value = b2b_tw_telekomgk_sentiment
ws.range('M39').value = b2b_tw_vodafoneb2b_sentiment
ws.range('S39').value = b2b_tw_telefonicade_x_sentiment
ws.range('Y39').value = b2b_tw_deutschetelekom_x_sentiment
ws.range('AE39').value = b2b_tw_vodafonede_x_sentiment
ws.range('AK39').value = b2b_tw_o2cando_sentiment

ws.range('G57').value = b2b_yt_telekombusiness_sentiment
ws.range('S57').value = b2b_yt_telefonicade_x_sentiment
ws.range('Y57').value = b2b_yt_deutschetelekomag_x_sentiment
ws.range('AE57').value = b2b_yt_vodafonede_x_sentiment
ws.range('AK57').value = b2b_yt_o2cando_sentiment
