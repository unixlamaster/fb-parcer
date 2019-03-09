import urllib2
import json
#import mysql.connector
import sqlite3
import sys    
import datetime
    
def connect_db():
    #fill this out with your db connection info
#    connection = mysql.connector.connect(user='JohnDoe', password='abc123',host = '127.0.0.1', database='facebook_data')
    connection = sqlite3.connect("mydatabase.db")
    cursor = connection.cursor()

    # Create tables
    cursor.execute("""CREATE TABLE if not exists page_info(
    id INT NOT NULL PRIMARY KEY,
    fb_id BIGINT UNSIGNED,
    likes BIGINT UNSIGNED,
    talking_about BIGINT UNSIGNED,
    username VARCHAR(40),
    time_collected datetime NOT NULL DEFAULT CURRENT_TIMESTAMP);""")
    cursor.execute("""CREATE TABLE if not exists post_info(
    id INT NOT NULL PRIMARY KEY,
    fb_post_id VARCHAR(200),
    message VARCHAR(800),
    likes_count BIGINT UNSIGNED,
    time_created DATETIME,
    shares BIGINT UNSIGNED,
    page_id INT NOT NULL,
    FOREIGN KEY(page_id)
        REFERENCES page_info(id)
        ON DELETE CASCADE);""")
    connection.commit()
    return connection
    
def create_post_url(graph_url, APP_ID, APP_SECRET):
    post_args = "/posts/?key=value&access_token=" + APP_ID + "|" + APP_SECRET
    post_url = graph_url + post_args

    return post_url

def render_to_json(graph_url):
    #render graph url call to JSON
    print "urlopen(",graph_url,")"
    web_response = urllib2.urlopen(graph_url)
    readable_page = web_response.read()
    json_data = json.loads(readable_page)

    return json_data

def scrape_posts_by_date(graph_url, date, post_data, APP_ID, APP_SECRET):
    #render URL to JSON
    page_posts = render_to_json(graph_url)

    #extract next page
    next_page = page_posts["paging"]["next"]

    #grab all posts
    page_posts = page_posts["data"]

    #boolean to tell us when to stop collecting
    collecting = True

    #for each post capture data
    for post in page_posts:
        try:
            likes_count = get_likes_count(post["id"], APP_ID, APP_SECRET)
            current_post = [post["id"], post["message"], likes_count,
                            post["created_time"], post["shares"]["count"]]

        except Exception:
            current_post = [ "error", "error", "error", "error"]

        if current_post[3] != "error":
            print date
            print current_post[3]
            if date <= current_post[3]:
                post_data.append(current_post)

            elif date > current_post[3]:
                print "Done collecting"
                collecting = False
                break


    #If we still don't meet date requirements, run on next page
    if collecting == True:
        scrape_posts_by_date(next_page, date, post_data, APP_ID, APP_SECRET)

    return post_data

def get_likes_count(post_id, APP_ID, APP_SECRET):
    #create Graph API Call
    graph_url = "https://graph.facebook.com/"
    likes_args = post_id + "/likes?summary=true&key=value&access_token" + APP_ID + "|" + APP_SECRET
    likes_url = graph_url + likes_args
    likes_json = render_to_json(likes_url)

    #pick out the likes count
    count_likes = likes_json["summary"]["total_count"]

    return count_likes
        
def main():
    APP_SECRET = sys.argv[2]
    APP_ID = sys.argv[1]
    #to find go to page's FB page, at the end of URL find username
    #e.g. http://facebook.com/walmart, walmart is the username
    list_companies = ["walmart", "cisco", "pepsi", "facebook"]
    graph_url = "https://graph.facebook.com/"

  #the time of last weeks crawl
    last_crawl = datetime.datetime.now() - datetime.timedelta(weeks=1)
    last_crawl = last_crawl.isoformat()
  
    #create db connection
    connection = connect_db()
    cursor = connection.cursor()

    # SQL statement for adding Facebook database
    insert_info = ("INSERT INTO page_info "
      "(fb_id, likes, talking_about, username)"
      "VALUES (%s, %s, %s, %s)")

    #SQL statement for adding post data
    insert_posts = ("INSERT INTO post_info "
                    "(fb_post_id, message, likes_count, time_created, shares, page_id)"
                    "VALUES (%s, %s, %s, %s, %s, %s)")

    for company in list_companies:
        current_page = graph_url + company

        #open public page in facebook graph api
        json_fbpage = render_to_json(current_page)

        #gather our page level JSON Data
        page_data = (json_fbpage["id"], json_fbpage["likes"],
                     json_fbpage["talking_about_count"],
                     json_fbpage["username"])
        print page_data

        #extract post data
        post_url = create_post_url(current_page, APP_ID, APP_SECRET)
        post_data = []
        post_data = scrape_posts_by_date(post_url, last_crawl, post_data, APP_ID, APP_SECRET)



        print post_data

        #insert the data we pulled into db
        cursor.execute(insert_info, page_data)

        #grab primary key
        last_key = cursor.lastrowid

        #loop through and insert data
        for post in post_data:
            post.append(last_key)
            cursor.execute(insert_posts, post)
            #commit the data to the db
            connection.commit()

    connection.close()
if __name__ == "__main__":
    main()
