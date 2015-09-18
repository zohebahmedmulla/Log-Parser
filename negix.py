import re
import pprint
import sqlite3
import datetime
import sys

def create_logs_table():
    # create_logs_table
    # Holds all the values
    # obtained from parsing
    # Logs
    try:
        conn = sqlite3.connect('test.db')
        conn.execute('''create table if not exists negix1
        (Body_Byte_Sent  INT,
        Http_Referer     TEXT,
        Http_user_Agent  TEXT,
        Remote_Addr      CHAR(50),
        Remote_User      CHAR(50),
        Request          CHAR(100),
        Status           INT,
        Time_Local       DATETIME
        );''')
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn.close()
        

def create_httpstatus_table():
    # create_httpstatus_table
    # Holds all the HTTPSTATUS
    # Codes garnered from processing
    try:
        conn = sqlite3.connect('test.db')
        conn.execute('''create table if not exists httpstatus
        (Total_count     INT,
        status2          INT,
        status3          INT,
        status4          INT,
        status5          INT
        );''')
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn.close()
    
def insert_httpstatus_table():
    two = 0          # Variable holds the 2XX Httpstatus codes
    three = 0        # Variable holds the 3XX Httpstatus codes
    four = 0         # Variable holds the 4XX Httpstatus codes
    five = 0         # Variable holds the 5XX Httpstatus codes
    count_status = 0 # Variable holds the count all the status
    # Next few steps getting the values from negix1 for 2XX,3XX,4XX,5XX Httpstatus codes
    try:
        conn = sqlite3.connect('test.db')
        cursor = conn.execute("SELECT count(*) from negix1 where Status like'2%'")
        for row in cursor:
            two = row[0]
        cursor = conn.execute("SELECT count(*) from negix1 where Status like'3%'")
        for row in cursor:
            three = row[0]
        cursor = conn.execute("SELECT count(*) from negix1 where Status like'4%'")
        for row in cursor:
            four = row[0]
        cursor = conn.execute("SELECT count(*) from negix1 where Status like'5%'")
        for row in cursor:
            five = row[0]
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    else:
        count_status = five + four + three + two # Total count
        #Inserting values in httpstatus table
        try:
            conn.execute("INSERT INTO httpstatus(Total_count, status2, status3, status4, status5)values (?, ?, ?, ?, ?)",(count_status, two, three, four, five))
        except Exception, err:
            sys.stderr.write('ERROR: %sn' % str(err))
        else:
            conn.commit()
    finally:
        conn.close()

# Method for processing failures
def Processing_failures():
    count = 0  # Holds the count error httpstatus codes 4XX and 5XX
    # Fethches data from httpstatus
    # For httpscodes 4XX and 5XX
    # Tottal of the above two is returned
    try:
        conn1 = sqlite3.connect('test.db')
        cursor = conn1.execute("SELECT Total_count, status2, status3, status4, status5 from httpstatus")
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    else:
        for row in cursor:
            count = int(row[3]) + int(row[4])
        return count
    finally:
        conn1.close()

# Method is to display values from negix1 table
def display_logs():
    try:
        conn1 = sqlite3.connect('test.db')
        cursor = conn1.execute("SELECT Body_Byte_Sent, Http_Referer, Http_user_Agent, Remote_Addr, Remote_User, Request, Status, Time_Local from negix1")
        for row in cursor:
            print row[0]
            print row[1]
            print row[2]
            print row[3]
            print row[4]
            print row[5]
            print row[6]
            print row[7]
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn1.close()

# Method is to delete both negix1 and httpstatus table
# used for internal testing
def delete_table():
    try:
        conn1 = sqlite3.connect('test.db')
        cursor = conn1.execute("DELETE FROM negix1")
        cursor1 = conn1.execute("DELETE FROM httpstatus")
        conn1.commit()
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))        
    finally:
        conn1.close()

# Method is to insert values in to negix1 table
def insert_logs(values):
    try:
        conn = sqlite3.connect('test.db')
        for i in range(0, len(values)):
            body_bytes_sent = int(values[i][5])
            http_referer = values[i][6]
            http_user_agent = values[i][7]
            remote_addr = values[i][0]
            remote_user = values[i][1]
            request = values[i][3][5:-9]
            status = int(values[i][4])
            time_local = values[i][2]
            d=datetime.datetime.strptime(time_local[3:6],"%b")
            # changing the format to insert in to db
            yyyymmdd = time_local[7:11] + "-" + str(d.month).zfill(2) + "-" + time_local[:2]+" " + time_local[12:14]+":"+time_local[15:17]+":"+time_local[18:20]
            conn.execute("INSERT INTO negix1(Body_Byte_Sent,Http_Referer,Http_user_Agent,Remote_Addr,Remote_User,Request,Status,Time_Local)values (?, ?, ?, ?, ?, ?, ?, ?)",
            (body_bytes_sent,http_referer,http_user_agent,remote_addr,remote_user,request,status,yyyymmdd))
            conn.commit()
            # creating and callinf table httpstatus
            create_httpstatus_table()
            insert_httpstatus_table()
            conn.commit()
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn.close()

# Initiation of processing logic for access logs
def process_log(log):
    # Passing log data as paramater for method get_requests
    # output of get_requests method will contain the log data in the standarard format
    requests = get_requests(log)
    # calling method insert_logs to insert the read values to database
    create_logs_table()
    insert_logs(requests)
    return requests

# contains number of log entries processed
def log_count(log):
    count = 0
    try:
        conn1 = sqlite3.connect('test.db')
        # getting the total count from the httpstatus table
        cursor = conn1.execute("SELECT Total_count from httpstatus")
        for row in cursor:
            count = row[0]
        return count
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn1.close()
        
# Actual processing logic 
def get_requests(f):
    log_line = f.read()
    # regular expression to parse the logs
    conf = '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
    regex = ''.join('(?P<' + g + '>.*?)' if g else re.escape(c) for g, c in re.findall(r'\$(\w+)|(.)', conf))
    # Call the method to fetch data that matches the condition
    requests = find(regex, log_line, None)
    return requests

# Returns the log entries that matches the specifies regular expression
def find(pat, text, match_item):
    match = re.findall(pat, text)
    return match

# method to display count of the log entries based on httpstatus code
def httpstatus_count(requests, code):
    count_status = 0
    try:
        conn = sqlite3.connect('test.db')
        # fetching the count from db for given codes
        cursor = conn.execute("SELECT count(*) from negix1 where Status="+str(code))
        for row in cursor:
            count_status = row[0]
        return count_status
    except Exception, err:
        sys.stderr.write('ERROR: %sn' % str(err))
    finally:
        conn.close()

# Http request possessing same IP, same date and same agent
def unique_hit(requests, ddmmmyyyy):
    # get requested files with req
    requested_files = [] # list with unique request
    unique_visitors = [] # list of ip and user agent
    for req in requests:
        # checking entries with given date
        if req[2][0:11] == ddmmmyyyy:
            # checking if ip and user agent are same
            # by checking if ip and user agent already present
            # if no insert in to list requested_files and unique_visitors
            if unique_visitors.count(req[0] + ' ' + req[7]) == 0:
                requested_files.append(req[3][5:-9])
                unique_visitors.append(req[0] + ' ' + req[7])
    return requested_files

# request with http satus is 200, with same IP,user agent with n seconds
def page_view(requests, ddmmmyyyyhhmmssf, nsec):
    # get requested files with req
    requested_files = []
    unique_visitors = []
    for req in requests:
        # converting dae in dd/mmm/yyyy:HH:MM:SS f format
        starttime = datetime.datetime.strptime(ddmmmyyyyhhmmssf, '%d/%b/%Y:%H:%M:%S +%f')
        endtime = starttime + datetime.timedelta(seconds = nsec)
        logtime = datetime.datetime.strptime(req[2], '%d/%b/%Y:%H:%M:%S +%f')
        # checking for http status
        if int(req[4]) >= 200 and int(req[4]) < 300:
            # checking entries with given date
            if starttime < logtime < endtime:
                # checking if ip and user agent are same
                # by checking if ip and user agent already present
                # if no insert in to list requested_files and unique_visitors
                if unique_visitors.count(req[0] + ' ' + req[7]) == 0:
                    requested_files.append(req[3][5:-9])
                    unique_visitors.append(req[0] + ' ' + req[7])
    return requested_files

def url_pattern_predictor(requesturl):
    return requesturl.partition('/')[0]
    
# provides request counts for page_view and unique_hit
def page_count(files):
    # file occurrences in requested files
    d = {}
    for file in files:
        d[file] = d.get(file,0) + 1
    return d

if __name__ == '__main__':
    create_httpstatus_table()
  
   
    #log_file = open('C:\Users\zoheb\Desktop\project\logs.txt', 'r')
    #print httpstatus_count(process_log(log_file),200)
    #print log_count(process_log(log_file))
    #print Processing_failures()
'''
    #nginx access log, standard format
    start=datetime.datetime.now()
    log_file = open('C:\Users\zoheb\Desktop\project\logsCopy.txt', 'r')
    print(insert_logs(process_log(log_file)))
    end=datetime.datetime.now()
    print end-start
'''
    
