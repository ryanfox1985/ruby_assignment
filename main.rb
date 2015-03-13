require 'base64'
require 'zlib'
require 'json'
require 'csv'

program_starts = Time.now
puts "Begin ===>>#{program_starts}"

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
  hash_users_google_per_day = {}

  arr_pageviews_facebook = []
  avg_pageviews_facebook = 0
  num_pageviews_facebook = 0

  CSV.foreach(file_name, { col_sep: "\t" }) do |row|
    domain = row[0]
    date = row[1]
    block = row[2]

    strJSON = uncompress_block(block)
    user_traces = JSON.parse(strJSON)

    if domain.include?('facebook.com')
      hash_unique_users_facebook_per_day[date] = {} if hash_unique_users_facebook_per_day[date].nil?

      user_traces.each do |user_id, times|
        hash_unique_users_facebook[user_id] = true
        hash_unique_users_facebook_per_day[date][user_id] = true

        times.each do |time_slice, arr_sec_pageviews|
          arr_pageviews_facebook << arr_sec_pageviews[1].to_i
          avg_pageviews_facebook += arr_sec_pageviews[1].to_i
          num_pageviews_facebook += 1
        end
      end
    end

    if domain.include?('google.')
      hash_users_google_per_day[date] = [] if hash_users_google_per_day[date].nil?

      user_traces.each do |user_id, times|
        times.each do |time_slice, arr_sec_pageviews|
          if time_slice.to_i >= 20*4 && time_slice.to_i < 23*4
            hash_users_google_per_day[date] << [user_id, arr_sec_pageviews[0]]
          end
        end
      end
    end

    puts "Date and domain => #{date} - #{domain}"
  end

  puts "\n\n"
  puts "====================="
  puts "-------RESULTS-------"
  puts "====================="

  puts "1. Unique users, per day, on facebook.com:"
  hash_unique_users_facebook_per_day.each do |date, users|
    puts "Date: #{date} -- users: #{users.length}"
  end

  puts "2. Per­day average number of seconds spent on Google properties (google.*) between 20:00 and 23:00, by people that also have visited facebook.com."
  hash_users_google_per_day.each do |date, arr_users_secs|
    average = 0
    counter = 0

    arr_users_secs.each do |user_sec|
      if hash_unique_users_facebook.has_key? user_sec[0]
        average += user_sec[1].to_i
        counter += 1
      end
    end

    average /= counter.to_f if counter > 0
    puts "Date: #{date} -- avg seconds: #{average}"
  end

  puts "3. Calculate the standard deviation of the amount of pageviews on facebook.com, based on all data."
  avg_pageviews_facebook /= num_pageviews_facebook.to_f if num_pageviews_facebook > 0
  sum_squares = arr_pageviews_facebook.inject(0) { |result, amount| result + (amount - avg_pageviews_facebook) ** 2 }

  std_pageviews_facebook = 0
  std_pageviews_facebook = Math.sqrt(sum_squares/num_pageviews_facebook.to_f) if num_pageviews_facebook > 0

  puts "Avg pageviews: #{avg_pageviews_facebook}"
  puts "Std pageviews: #{std_pageviews_facebook}"
end

if File.exist?('domain_spread_cache')
  process_file('domain_spread_cache')
else
  puts "Error ==> file not found ./domain_spread_cache"
end

program_ends = Time.now
puts "End ===>>#{program_ends}"
puts "Time in seconds => #{program_ends - program_starts}"
