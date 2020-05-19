import os
import time
import tweepy
import random
import csv
import sys

seq_num = 0
tweet_res = []
res_size = 100
tag_dict = {}

outfile = sys.argv[2]
csv_file = open(outfile, 'w')
csv_file.close()
#csv_write = csv.writer(csv_file, delimiter=',', lineterminator='\n')


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):

        global seq_num
        global res_size
        global tweet_res

        tags = status.entities['hashtags']

        if (len(tags) != 0):

            seq_num += 1

            if (len(tweet_res) < res_size):

                tweet_res.append(status)

                for tag in tags:
                    tag_text = tag['text']
                    if tag_text in tag_dict:
                        tag_dict[tag_text] += 1
                    else:
                        tag_dict[tag_text] = 1

            else:

                rnd_num = random.random()

                if (rnd_num < (res_size / seq_num)):  # keep

                    ind = random.randrange(res_size)

                    ##replacing the tweet at that ind

                    prev_tweet = tweet_res[ind]

                    taged = prev_tweet.entities['hashtags']

                    for tag in taged:
                        tag_text = tag['text']
                        tag_dict[tag_text] -= 1
                        if (tag_dict[tag_text] == 0):
                            del tag_dict[tag_text]

                    tweet_res[ind] = status

                    for tag in tags:
                        tag_text = tag['text']
                        if tag_text in tag_dict:
                            tag_dict[tag_text] += 1
                        else:
                            tag_dict[tag_text] = 1

                else:
                    pass

            tag_list = tag_dict.items()

            tag_list = sorted(tag_list, key=lambda x: (-x[1], x[0]))
            
            tag_freq = set()
            
            ind = len(tag_list)-1
            
            for i,tag in enumerate(tag_list):
                tag_freq.add(tag[1])          
                if (len(tag_freq) > 3):
                    ind = i
                    break
            if (len(tag_list)-1 != ind):
                tag_list = tag_list[:ind]
                    
                
            
            csv_file = open(outfile, 'a+')
            
            csv_file.write("The number of tweets with tags from the beginning: " + str(seq_num) + "\n" )
            
            for t in tag_list:
                csv_file.write( str(t[0])+ " : " + str(t[1]) + "\n")
                
            csv_file.write( "\n" )
            
            csv_file.close()
            
            
            
#            for line in text:
 #               file.write(line)
  #              file.write('\n')
                
                

            print(seq_num)
            print(tag_list)


if __name__ == "__main__":
    api_key = "NDMtNfoEZ9OnxyZgHeLW52D1A"
    api_key_secret = "v7UaL35Z8jQ2aFtVEeLNQFgjLqfpWJF6TVOUktLRB5Nog5P2c2"

    access_token = "1252278609466671106-wWvIt8eoztS8yLc7sIVJ5MXykEl00h"
    access_token_secret = "7usMT9dyUHgOOdsxSrJJDm96AwXnojSY4erlzOb2SuUbN"

    auth = tweepy.OAuthHandler(api_key, api_key_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)
    #myStream.filter(locations=[-122.75,36.8,-121.75,37.8,-74,40,-73,41])
    myStream.filter(track=["#",'corona'])
