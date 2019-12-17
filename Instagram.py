# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:44:08 2019

@author: Ritvik.Tiwari
"""

import requests
import json
import mysql.connector
from mysql.connector import Error
import pyodbc



def urlRequests():
    pageRequest = requests.get("https://graph.facebook.com/v3.2/me/accounts?access_token=EAAhxzjscCrABAKpzVaJZAL6wMDk8gl4ag9HiF4GHjXod8fqZC5m6JtiYu7YIZBC88kmPNJOMaUjCCGrOZABLIpgIpJMsjUJxkLy7LUqDF35lf8c8QbuhFFrQyqJKYX2iN70iHpStRj5oC710Xk0RSIOZAuuZAyBfGyE9v1JC0ZBwiqynFXpd6Iq").text
    pageContent = json.loads(pageRequest)
    #print(pageContent)
    
    instaRequest = requests.get("https://graph.facebook.com/v3.2/2333347030036724?fields=instagram_business_account&access_token=EAAhxzjscCrABAKpzVaJZAL6wMDk8gl4ag9HiF4GHjXod8fqZC5m6JtiYu7YIZBC88kmPNJOMaUjCCGrOZABLIpgIpJMsjUJxkLy7LUqDF35lf8c8QbuhFFrQyqJKYX2iN70iHpStRj5oC710Xk0RSIOZAuuZAyBfGyE9v1JC0ZBwiqynFXpd6Iq").text
    instaContent = json.loads(instaRequest)
    #print(instaContent)
    
    instaUserInfoRequest = requests.get("https://graph.facebook.com/v3.2/17841408529557130?fields=id,username,ig_id,biography,followers_count,follows_count,media_count,name,profile_picture_url,website&access_token=EAAhxzjscCrABAKpzVaJZAL6wMDk8gl4ag9HiF4GHjXod8fqZC5m6JtiYu7YIZBC88kmPNJOMaUjCCGrOZABLIpgIpJMsjUJxkLy7LUqDF35lf8c8QbuhFFrQyqJKYX2iN70iHpStRj5oC710Xk0RSIOZAuuZAyBfGyE9v1JC0ZBwiqynFXpd6Iq").text
    instaUserInfo = json.loads(instaUserInfoRequest)
    #print(instaUserInfo)
    
    instaMediaRequest = requests.get("https://graph.facebook.com/v3.2/17841408529557130/media?access_token=EAAhxzjscCrABAKpzVaJZAL6wMDk8gl4ag9HiF4GHjXod8fqZC5m6JtiYu7YIZBC88kmPNJOMaUjCCGrOZABLIpgIpJMsjUJxkLy7LUqDF35lf8c8QbuhFFrQyqJKYX2iN70iHpStRj5oC710Xk0RSIOZAuuZAyBfGyE9v1JC0ZBwiqynFXpd6Iq").text
    instaMedia = json.loads(instaMediaRequest)
    mediaIdArray = []
    data = {}
    data['insta'] = []
    for value in instaMedia['data']:
        instaMediaInfoRequest = requests.get("https://graph.facebook.com/v3.2/"+value['id']+"?fields=id,media_type,media_url,owner,timestamp,username,thumbnail_url,shortcode,permalink,like_count,comments_count,comments,children,caption&access_token=EAAhxzjscCrABAKpzVaJZAL6wMDk8gl4ag9HiF4GHjXod8fqZC5m6JtiYu7YIZBC88kmPNJOMaUjCCGrOZABLIpgIpJMsjUJxkLy7LUqDF35lf8c8QbuhFFrQyqJKYX2iN70iHpStRj5oC710Xk0RSIOZAuuZAyBfGyE9v1JC0ZBwiqynFXpd6Iq").text
        instaMediaInfo = json.loads(instaMediaInfoRequest)
        mediaIdArray.append(instaMediaInfo)
        temp =mediaIdArray[0]['timestamp']
        temp =temp.split("T")
        print("temp valuee is",temp[0])



    connect(mediaIdArray)
    
def connect(mediaData):
    #Connecting to database
    try:
        server = 'GTIGGNLAP325'
        database = 'ptest'
        user = 'SqlAdmin'
        password = 'root@123'
        con = pyodbc.connect('DRIVER= {SQL Server};SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ password)
        print("Connection successful")
            
        #if con.is_connected():
            #insert Data            
        #cursor = con.cursor()
        cursor = con.cursor()
            # Create Table
        for text in mediaData:
            print (text)
            temp =text['timestamp']
            temp =temp.split("T")
            timestamp =temp[0]
            cursor.execute('''INSERT INTO ptest.dbo.insta (media_type,username,like_count,caption,timestamp) VALUES (?,?,?,?,?)''', (text['media_type'], text['username'], text['like_count'], text['caption'],timestamp ))         
                 #query = ('''INSERT INTO ptest.dbo.insta (id,media_type,username,like_count,caption,timestamp) VALUES (?,?,?,?,?,?)''', (mediaData))  
                 #insert_tuple = ()
                 #cursor.execute(query,insert_tuple)
                #results = cursor.fetchall()
                 #print(results)
                 #for row in results:
                     #print(row[0])
            print("Number of rows updated: %d" % cursor.rowcount)
                 #cursor.execute ("Select * from ptest.dbo.insta")
            con.commit()
            
            
    except Error as e:
        print(e)

    cursor.close()
    #cursor1.close()
    con.close()

if __name__== '__main__':
    
   urlRequests()