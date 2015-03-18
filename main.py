import datetime
from datetime import datetime
import os.path
import csv
import sys
import base64
import zlib
import json
import math

program_starts = datetime.now()
print("Begin ===>>" + str(program_starts))

def uncompress_block(block):
    decoded = base64.b64decode(block)
    buf = zlib.decompress(decoded)
    return buf


def process_file(file_name):
      hash_unique_users_facebook = {}
      hash_unique_users_facebook_per_day = {}
      hash_users_google_per_day = {}

      arr_pageviews_facebook = []
      avg_pageviews_facebook = 0
      num_pageviews_facebook = 0

      with open(file_name, 'r') as csvfile:
          csv.field_size_limit(sys.maxsize)
          spamreader = csv.reader(csvfile, delimiter='\t', quotechar='"')
          for row in spamreader:
              domain = row[0]
              date = row[1]
              block = row[2]

              strJSON = uncompress_block(block)
              user_traces = json.loads(strJSON)

              if "facebook.com" in domain:
                  if not date in hash_unique_users_facebook_per_day:
                      hash_unique_users_facebook_per_day[date] = {}

                  for user_id, times in user_traces.iteritems():
                      hash_unique_users_facebook[user_id] = 1
                      hash_unique_users_facebook_per_day[date][user_id] = 1

                      for time_slice, arr_sec_pageviews in times.iteritems():
                          arr_pageviews_facebook.append(int(arr_sec_pageviews[1]))
                          avg_pageviews_facebook += int(arr_sec_pageviews[1])
                          num_pageviews_facebook += 1

              if "google." in domain:
                  if not date in hash_users_google_per_day:
                      hash_users_google_per_day[date] = []

                  for user_id, times in user_traces.iteritems():
                      for time_slice, arr_sec_pageviews in times.iteritems():
                          if int(time_slice) >= 20*4 and int(time_slice) < 23*4:
                              hash_users_google_per_day[date].append([user_id, arr_sec_pageviews[0]])

              print("Date and domain => " + date + " - " + domain)


      print("\n\n")
      print("=====================")
      print("-------RESULTS-------")
      print("=====================")

      print("1. Unique users, per day, on facebook.com:")
      for date, users in hash_unique_users_facebook_per_day.iteritems():
          print("Date: " + date + " -- users: " + str(len(users)))

      print("2. Perday average number of seconds spent on Google properties (google.*) between 20:00 and 23:00, by people that also have visited facebook.com.")
      for date, arr_users_secs in hash_users_google_per_day.iteritems():
          average = 0
          counter = 0

          for user_sec in arr_users_secs:
              if user_sec[0] in hash_unique_users_facebook:
                  average += int(user_sec[1])
                  counter += 1

          if counter > 0:
              average /= float(counter)
          print("Date: " + date + " -- avg seconds: " + str(average))



      print("3. Calculate the standard deviation of the amount of pageviews on facebook.com, based on all data.")

      if num_pageviews_facebook > 0:
          avg_pageviews_facebook /= float(num_pageviews_facebook)

      sum_squares = reduce(lambda result, amount: result + math.pow((amount - avg_pageviews_facebook), 2), arr_pageviews_facebook)
      std_pageviews_facebook = 0
      if num_pageviews_facebook > 0:
          std_pageviews_facebook = math.sqrt(sum_squares/float(num_pageviews_facebook))

      print("Avg pageviews: " + str(avg_pageviews_facebook))
      print("Std pageviews: " + str(std_pageviews_facebook))



if os.path.exists('domain_spread_cache'):
  process_file('domain_spread_cache')
else:
  print("Error ==> file not found ./domain_spread_cache")


program_ends = datetime.now()
print("End ===>>" + str(program_ends))
print("Time in seconds => " + str(program_ends - program_starts))
