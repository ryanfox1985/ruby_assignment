require 'base64'
require 'zlib'
require 'json'

puts "Begin ===>>#{Time.now}"

def get_more_data(fileObj)
  appended_data = ''

  end_data = false
  while !end_data
    line = fileObj.gets

    if line.include?('"')
      end_data = true
    else
      appended_data += line[0..-2]
    end
  end

  appended_data
end

def uncompress_block(block)
  decoded = Base64.decode64(block)

  zstream = Zlib::Inflate.new
  buf = zstream.inflate(decoded)
  zstream.finish
  zstream.close

  buf
end

def process_file(file_name)
  hash_unique_users_facebook = {}
  hash_unique_users_facebook_per_day = {}
  hash_unique_users_google_per_day = {}
  arr_pageviews_facebook = []

  fileObj = File.new(file_name, "r")
  while (line = fileObj.gets)

    domain = line.split(' ')[0]
    date = line.split(' ')[1]
    block = line.split(' ')[2][1..-1]

    block = block + get_more_data(fileObj)

    strJSON = uncompress_block(block)
    user_traces = JSON.parse(strJSON)

    if domain.include?('facebook.com')
      hash_unique_users_facebook_per_day.store date, {}

      user_traces.each do |user_id, times|
        hash_unique_users_facebook[user_id] = true
        hash_unique_users_facebook_per_day[date][user_id] = true

        times.each do |time_slice, arr_sec_pageviews|
          arr_pageviews_facebook.push arr_sec_pageviews[1].to_i
        end
      end
    end

    if domain.include?('google.')
      hash_unique_users_google_per_day.store date, []

      user_traces.each do |user_id, times|
        times.each do |time_slice, arr_sec_pageviews|
          if time_slice.to_i >= 20*4 and time_slice.to_i <= 23*4
            hash_unique_users_google_per_day[date].push [user_id, arr_sec_pageviews[0]]
          end
        end
      end
    end

    puts "Date and domain => #{date} - #{domain}"
  end

  fileObj.close

  puts "\n\n"
  puts "====================="
  puts "-------RESULTS-------"
  puts "====================="

  puts "1. Unique users, per day, on facebook.com:"
  hash_unique_users_facebook_per_day.each do |date, users|
    puts "DATE: #{date} -- users: #{users.length}"
  end

  puts "2. Per­day average number of seconds spent on Google properties (google.*) between 20:00 and 23:00, by people that also have visited facebook.com."
  hash_unique_users_google_per_day.each do |date, arr_users_secs|
    average = 0
    counter = 0

    arr_users_secs.each do |user_sec|
      if hash_unique_users_facebook.has_key? user_sec[0]
        average += user_sec[1].to_i
        counter += 1
      end
    end

    average /= counter.to_f if counter > 0
    puts "DATE: #{date} -- avg seconds: #{average}"
  end

  puts "3. Calculate the standard deviation of the amount of pageviews on facebook.com, based on all data."
  avg_pageviews_facebook = 0
  std_pageviews_facebook = 0

  arr_pageviews_facebook.each do |amount|
    avg_pageviews_facebook += amount
  end

  avg_pageviews_facebook /= arr_pageviews_facebook.length.to_f unless arr_pageviews_facebook.empty?

  sum_squares = 0
  arr_pageviews_facebook.each do |amount|
    sum_squares += (amount - avg_pageviews_facebook) ** 2
  end

  unless arr_pageviews_facebook.empty?
    std_pageviews_facebook = Math.sqrt(sum_squares/arr_pageviews_facebook.length.to_f)
  end

  puts "Avg pageviews: #{avg_pageviews_facebook}"
  puts "Std pageviews: #{std_pageviews_facebook}"
end

if File.exist?('domain_spread_cache')
  process_file('domain_spread_cache')
else
  puts "Error ==> file not found ./domain_spread_cache"
end

puts "End ===>>#{Time.now}"
