# -*- coding: utf-8 -*-
"""
Created on Wed Oct  9 11:42:53 2019

@author: Ritvik.Tiwari
"""

import requests
import facebook
import mysql.connector
from mysql.connector import Error
import pyodbc

def get_info():
        app_id = '789275491473993'
        app_secret = 'ae92c879f107afb77b39ae558391fdb5'
        user_short_token = 'EAALN14VMmkkBAMJlnZBu0JqaBOqgXAifYahh7NouKMYJeI4rdXntviUT0dtWIhKhSiXLvcA849biZBy1zR6yDTxrVd0MQnmlhvIPOICnprkZBxqWkbxdRZCQQ1nxVZABSxEs5irfOdDZB9A9FqZA8fDlMiFolHngy6AkjaX1lS1XyTlZBWAukbPP'
        access_token_url = "https://graph.facebook.com/oauth/access_token?grant_type=fb_exchange_token&client_id={}&client_secret={}&fb_exchange_token={}".format(app_id, app_secret, user_short_token)
        
        r = requests.get(access_token_url)
        access_token_info = r.json()
        user_long_token = access_token_info['access_token']
        #print(access_token_info)
        
        graph = facebook.GraphAPI(access_token=user_long_token, version="3.0")
        pages_data = graph.get_object("/me/accounts")
        
        #print(pages_data)
        
        
        page_id = '621671057889190'
        page_token = None
        
        for item in pages_data['data']:
           if item['id'] == page_id:
              page_token = item['access_token']
              
        #print(page_token)
               
               
        graph = facebook.GraphAPI(access_token=page_token, version="3.0")
        
        #print(graph.get_object(id=page_id, fields='about, category'))
              
        
        page_token ='EAALN14VMmkkBAMJlnZBu0JqaBOqgXAifYahh7NouKMYJeI4rdXntviUT0dtWIhKhSiXLvcA849biZBy1zR6yDTxrVd0MQnmlhvIPOICnprkZBxqWkbxdRZCQQ1nxVZABSxEs5irfOdDZB9A9FqZA8fDlMiFolHngy6AkjaX1lS1XyTlZBWAukbPP'
        graph = facebook.GraphAPI(access_token=page_token, version="3.1")
        default_info = graph.get_object(id=page_id)
        #print(default_info)
        
        
        some_info = graph.get_object(id=page_id, fields='about, website')
        #print(some_info)
               
        
        page_token ='EAALN14VMmkkBAMJlnZBu0JqaBOqgXAifYahh7NouKMYJeI4rdXntviUT0dtWIhKhSiXLvcA849biZBy1zR6yDTxrVd0MQnmlhvIPOICnprkZBxqWkbxdRZCQQ1nxVZABSxEs5irfOdDZB9A9FqZA8fDlMiFolHngy6AkjaX1lS1XyTlZBWAukbPP'
        posts_25 = graph.get_object(id=page_id,
                                    fields='feed.fields(permalink_url,created_time,attachments{title, description},message)')
        
        
        print(posts_25)
        connect(posts_25['feed']['data'])
        
def connect(feedData):
    
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
        cursor = con.cursor()
            # Create Table
        for text in feedData:
                 print(text['permalink_url'])
                 #print(text['created_time'])
                 temp = []
                 created_time = text['created_time']
                 temp = created_time.split("T")
                 #print(temp)
                 created_time = temp[0]
                 print(temp)
                 A = 'message'
                 if A in text:
                     A = text['message']
                 else:
                     A  = 'null'
                 #print(text['icon'])
                 B = 'title'
                 C = 'description'
                 if 'attachments' in text:
                     if B in text['attachments']['data'][0]:
                         B = text['attachments']['data'][0]['title']
                     else:
                         B = 'null'
                    
                     if C in text['attachments']['data'][0]:
                         C = text['attachments']['data'][0]['description']
                     else:
                         C = 'null'
                 else:
                     B = 'null'
                     C = 'null'
                     #print(A, B, C)
                 cursor.execute ('''INSERT INTO ptest.dbo.GTfb(permalink_url,created_time,message,title,description) VALUES (?,?,?,?,?)''',(text['permalink_url'],created_time, A, B, C))
                 #cursor.execute(query, (A, B, C))
                 con.commit()
            
            
    except Error as e:
        print(e)

    cursor.close()
    con.close()

    return

if __name__== '__main__':
    get_info()
    