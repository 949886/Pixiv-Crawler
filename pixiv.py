import os
import re
import time
import json
import random
import datetime
import requests
import mysql.connector
from mysql.connector import errorcode
    
class PixivDatabase():
    __instance = {}   # Singleton

    def __init__(self):
        self.__dict__ = self.__instance
        self.state = 'Init'

        self.config = {
            'user': 'root',
            'password': 'root',
            'host': '127.0.0.1',
            'database': 'pixiv',
            'raise_on_warnings': True
        }

        if not hasattr(self, "connection"):
            self.connectDatabase()
        

    def __del__(self):
        if self.connection.is_connected():
            self.connection.close()
            print("Database closed successfully.")
        

    def connectDatabase(self):
        try: self.connection = mysql.connector.connect(**self.config)
        except mysql.connector.Error as error:
            if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif error.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else: print(error)
        else: print("Connect successfully.")


    def createDatabase(self):
        cursor = self.connection.cursor()
        try: cursor.execute("CREATE DATABASE `pixiv` CHARACTER SET 'utf8';")
        except mysql.connector.Error as err:
            print(err)
        else: print("Create database successfully.")
        finally: cursor.close()


    def createTables(self):
        self.connection.start_transaction()
        with open("tables.sql", 'r') as tables_sql:
            sqls = tables_sql.read().split(";")
            for sql in sqls:
                try:
                    cursor = self.connection.cursor() 
                    cursor.execute(sql)
                except mysql.connector.Error as err:
                    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                        print("Table already exists.")
                    else: print("Create table failed. " + err.msg)
                else: print("Create table successfully.")
                finally: cursor.close()
        self.connection.commit()

    
    def addUser(self, user):
        cursor = self.connection.cursor()
        sql = """INSERT INTO `user` 
        VALUES (
            %(id)s, 
            %(name)s, 
            %(account)s, 
            %(profile_image_urls)s, 
            %(comment)s, 
            %(webpage)s,
            %(gender)s, 
            %(birth_day)s, 
            %(region)s,
            %(job)s, 
            %(total_follow_users)s, 
            %(total_follower)s, 
            %(total_mypixiv_users)s, 
            %(total_illusts)s, 
            %(total_manga)s, 
            %(total_novels)s, 
            %(total_illust_bookmarks_public)s, 
            %(twitter_account)s, 
            %(twitter_url)s, 
            %(pawoo_url)s, 
            %(illustrations)s);
        """
        self.connection.start_transaction()
        try: cursor.execute(sql, user)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                print("Data exists. " + err.msg)
            else:
                print("Insert data failed. " + err.msg)
                print(user)
        else: print("Insert data successfully.")
        self.connection.commit()

    def addIllustrations(self, illusts):
        cursor = self.connection.cursor()
        sql = """INSERT INTO `illustration` 
        VALUES (
            %(id)s, 
            %(title)s, 
            %(type)s, 
            %(image_urls)s, 
            %(caption)s, 
            %(restrict)s,
            %(user)s, 
            %(tags)s, 
            %(tools)s,
            %(create_date)s, 
            %(page_count)s, 
            %(width)s, 
            %(height)s, 
            %(sanity_level)s, 
            %(meta_single_page)s, 
            %(meta_pages)s, 
            %(total_view)s, 
            %(total_bookmarks)s, 
            %(total_comments)s, 
            %(visible)s, 
            %(is_muted)s);
        """
        self.connection.start_transaction()
        for illust in illusts:
            try: cursor.execute(sql, illust)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_DUP_ENTRY:
                    print("Data exists. " + err.msg)
                else:
                    print("Insert data failed. " + err.msg)
                    print(illust)
            else: print("Insert data successfully.")
        self.connection.commit()
        
        # try: 
            
        # except mysql.connector.Error as err:
        #     print("Insert data failed. " + err.msg)
        # else: print("Insert data successfully.")
        # finally: cursor.close()



class Pixiv():
    def __init__(self):
        self.session = requests.session()
        self.oauth = None
        self.headers = {
            "Authorization": "",
            "App-Version": "7.1.15",
            "X-Client-Time":"2018-08-09T07:39:42+08:00",
            "App-OS": "ios",
            "X-Client-Hash": "b7da8ab45600a82002574cf66c705381",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "en-us",
            "Accept": "*/*",
            "User-Agent": "PixivIOSApp/7.1.15 (iOS 10.3.1; iPad5,1)",
            "Connection": "keep-alive",
            "App-OS-Version": "10.3.1"
        }

    def checkExpiration(self):
        if self.oauth == None or self.oauth["expire_time"] == None:
            return True
        if time.time() > int(self.oauth["expire_time"]):
            return True
        else: return False

        
    def postOauth(self):
        url = "https://oauth.secure.pixiv.net/auth/token"
        oauth_form = {
            "client_secret":"W9JZoJe00qPvJsiyCGT3CCtC6ZUtdpKpzMbNlUGP",
            "refresh_token":"Qx_cvgtaWrlgfSOK7qYg8_hsFSKKwAr4m6ibLgciDCA",
            "client_id":"KzEZED7aC0vird8jWyHM38mXjNTY",
            "device_token":"3fced3c44432af2628f66e435c31ba89",
            "get_secure_url":"true",
            "include_policy":"true",
            "grant_type":"refresh_token"
        }

        try: 
            request = requests.post(url, data =oauth_form, verify=False)
            response = request.json()

            self.oauth = response["response"]
            self.oauth["expire_time"] = time.time() + response["response"]["expires_in"]
            self.headers["Authorization"] = "Bearer " + self.oauth["access_token"]
            self.session.headers.update(self.headers)
        except Exception as e:
            print("Oauth error.")


    def getUserInfo(self, userID):
        url = "https://app-api.pixiv.net/v1/user/detail"
        params = {
            "filter": "for_ios",
            "user_id": userID
        }
        while True:
            if self.checkExpiration():
                self.postOauth()

            try:
                request = self.session.get(url, params=params, verify=False)
                response = request.json()
                if request.status_code == requests.codes.ok:
                    return response
                elif request.status_code == requests.codes.forbidden:
                    if response["error"]["message"] == "Rate Limit":
                        time.sleep(20)
                    else: break
                elif request.status_code == 404 or \
                         request.status_code == 500:
                    break
            except Exception as e:
                print("Get user info error.")
                time.sleep(1)
            finally: time.sleep(0.5)
        return {}

    def getUserIllustrations(self, userID, offset=0):
        illusts = []

        nextURL = "https://app-api.pixiv.net/v1/user/illusts?type=illust&filter=for_ios&user_id={}".format(userID)
        while nextURL:
            if self.checkExpiration():
                self.postOauth()

            try:
                request = self.session.get(nextURL,  verify=False)
                response = request.json()
                if request.status_code == requests.codes.ok:
                    illusts += response["illusts"]
                    nextURL = response["next_url"]
                elif request.status_code == requests.codes.forbidden:
                    if response["error"]["message"] == "Rate Limit":
                        time.sleep(20)
                    else: break
                elif request.status_code == 404 or \
                         request.status_code == 500: 
                    break
            except Exception as e:
                print("Parse illustrations error")
                time.sleep(1)
            finally: time.sleep(0.5)

        self.handleIllusts(illusts)
        return illusts


    def handleIllusts(self, illusts):
        for illust in illusts:
            for key, value in illust.items():
                # tags
                if key == "tags" and value != None:
                    tags = ""
                    for tag in illust["tags"]:
                        tags += tag["name"] + ","
                    illust["tags"] =  tags[:-1]
                # tools
                elif key == "tools" and value != None:
                    tools = ""
                    for tool in illust["tools"]:
                        tools += tool + ","
                    illust["tools"] =  tools[:-1]
                # date
                elif key == "create_date" and value != None:
                    illust["create_date"] = illust["create_date"][:-6]
                # user
                elif key == "user" and value != None and "id" in value:
                    illust["user"] = illust["user"]["id"]
                # json
                elif type(value) == list or type(value) == dict:
                    illust[key] = json.dumps(value)

        

def user_info(user, illusts):
    info = {}

    for key, value in user.items():
        if key == "user" and value != None:
            for key, value in user["user"].items():
                info[key] = value

        elif key == "profile" and value != None:
            for key, value in user["profile"].items():
                info[key] = value

    for key, value in info.items():
        if type(value) == list or type(value) == dict:
            info[key] = json.dumps(value)

    illusInfo = ""
    for illust in illusts:
        illusInfo += str(illust["id"]) + ","
    info["illustrations"] =  illusInfo[:-1]

    return info
        

pixiv = Pixiv()
# userInfo = pixiv.getUserInfo(2836080)
# illustrations = pixiv.getUserIllustrations(2836080)

db = PixivDatabase()
# db.createDatabase() // Run this if database not exists.
# db.createTables()
# db.addUser(user_info(userInfo, illustrations))
# db.addIllustrations(illustrations)

for i in range(1, 99999999):
    print("Current user: " + str(i))

    userInfo = pixiv.getUserInfo(i)
    illustrations = pixiv.getUserIllustrations(i)

    db.addUser(user_info(userInfo, illustrations))
    db.addIllustrations(illustrations)

    # time.sleep(0.5)
