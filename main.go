package main

import (
  "fmt"
  "encoding/csv"
  "time"
  "os"
  "bytes"
  "strings"
  "strconv"
  "math"
  "encoding/json"
  "encoding/base64"
  "compress/zlib"
)

func exist_file(file_name string) bool {
  if _, err := os.Stat("domain_spread_cache"); err != nil {
    if os.IsNotExist(err) {
      fmt.Println("Error ==> file not found ./domain_spread_cache")
      return false
    }
  }

  return true
}

func uncompress_block(block string) string {
  decoded, _ := base64.StdEncoding.DecodeString(block)
	b := bytes.NewReader(decoded)

	reader, _ := zlib.NewReader(b)
  buf := new(bytes.Buffer)
  buf.ReadFrom(reader)
  s := buf.String() // Does a complete copy of the bytes in the buffer.

  reader.Close()

  return s
}

func process_file(file_name string) {
  var arr_pageviews_facebook []float64
  avg_pageviews_facebook := float64(0)
  num_pageviews_facebook := 0

  var hash_unique_users_facebook map[string]bool
  hash_unique_users_facebook = make(map[string]bool)

  var hash_unique_users_facebook_per_day map[string]map[string]bool
  hash_unique_users_facebook_per_day = make(map[string]map[string]bool)

  var hash_users_google_per_day map[string][]interface{}
  hash_users_google_per_day = make(map[string][]interface{})

  csvfile, err := os.Open(file_name)

  if err != nil {
     fmt.Println(err)
     return
  }

  defer csvfile.Close()
  reader := csv.NewReader(csvfile)

  reader.Comma = '\t'
  reader.FieldsPerRecord = -1 // see the Reader struct information below
  rawCSVdata, err := reader.ReadAll()

  if err != nil {
     fmt.Println(err)
     os.Exit(1)
  }

  for _, row := range rawCSVdata {
    domain := row[0]
    date := row[1]
    block := row[2]

    strJSON := uncompress_block(block)

    var data interface{}
    json.Unmarshal([]byte(strJSON), &data)
    user_traces := data.(map[string]interface{})


    if strings.Contains(domain, "facebook.com") {
      _, exist := hash_unique_users_facebook_per_day[date]
      if !exist {
        hash_unique_users_facebook_per_day[date] = make(map[string]bool)
      }

      for user_id, times_interface := range user_traces {
        hash_unique_users_facebook[user_id] = true
        hash_unique_users_facebook_per_day[date][user_id] = true

        times := times_interface.(map[string]interface{})
        for _, arr_sec_pageviews_interface := range times {
          arr_sec_pageviews := arr_sec_pageviews_interface.([]interface {})

          arr_pageviews_facebook = append(arr_pageviews_facebook, arr_sec_pageviews[1].(float64))
          avg_pageviews_facebook += arr_sec_pageviews[1].(float64)
          num_pageviews_facebook += 1
        }
      }
    }


    if strings.Contains(domain, "google.") {
      for user_id, times_interface := range user_traces {
        times := times_interface.(map[string]interface{})
        for time_slice_str, arr_sec_pageviews_interface := range times {
          time_slice, _ := strconv.ParseInt(time_slice_str, 0, 64)
          if time_slice >= 20*4 && time_slice < 23*4 {
            arr_sec_pageviews := arr_sec_pageviews_interface.([]interface {})

            var arr_sec_pageviews_save []interface{}
            arr_sec_pageviews_save = append(arr_sec_pageviews_save, user_id)
            arr_sec_pageviews_save = append(arr_sec_pageviews_save, arr_sec_pageviews[0].(float64))

            hash_users_google_per_day[date] = append(hash_users_google_per_day[date], arr_sec_pageviews_save)
          }
        }
      }
    }

    fmt.Printf("Date and domain => %s - %s\n", date, domain)
  }


  fmt.Printf("\n\n")
  fmt.Printf("=====================\n")
  fmt.Printf("-------RESULTS-------\n")
  fmt.Printf("=====================\n")

  fmt.Printf("1. Unique users, per day, on facebook.com:\n")
  for date, users := range hash_unique_users_facebook_per_day {
    fmt.Printf("Date: %s -- users: %d\n", date, len(users))
  }

  fmt.Printf("2. Per­day average number of seconds spent on Google properties (google.*) between 20:00 and 23:00, by people that also have visited facebook.com.\n")
  for date, arr_users_secs := range hash_users_google_per_day {
    average := float64(0)
    counter := 0

    for _, user_sec_interface := range arr_users_secs {
      user_sec := user_sec_interface.([]interface {})

      _, exist := hash_unique_users_facebook[user_sec[0].(string)]
      if exist {
        average += user_sec[1].(float64)
        counter += 1
      }
    }

    if counter > 0 {
      average /= float64(counter)
    }

    fmt.Printf("Date: %s -- avg seconds: %f\n", date, average)
  }



  fmt.Printf("3. Calculate the standard deviation of the amount of pageviews on facebook.com, based on all data.\n")
  if num_pageviews_facebook > 0 {
    avg_pageviews_facebook /= float64(num_pageviews_facebook)
  }

  sum_squares := float64(0)
  for _, amount := range arr_pageviews_facebook {
    diff := amount - avg_pageviews_facebook
    sum_squares += float64(math.Pow(diff, float64(2)))
  }

  std_pageviews_facebook := float64(0)
  if num_pageviews_facebook > 0 {
    std_pageviews_facebook := math.Sqrt(float64(sum_squares)/float64(num_pageviews_facebook))

    fmt.Printf("Avg pageviews: %f\n", avg_pageviews_facebook)
    fmt.Printf("Std pageviews: %f\n", std_pageviews_facebook)
  } else {
    fmt.Printf("Avg pageviews: %f\n", avg_pageviews_facebook)
    fmt.Printf("Std pageviews: %f\n", std_pageviews_facebook)
  }

}

func main() {
  program_starts := time.Now()
  fmt.Println("Begin ===>>", program_starts)

  if exist_file("domain_spread_cache") {
    process_file("domain_spread_cache")
  }

  program_ends := time.Now()
  fmt.Println("End ===>>", program_ends)
  fmt.Println("Time in seconds =>", time.Since(program_starts))
}
