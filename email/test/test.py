import MySQLdb

db = MySQLdb.connect(host="pit1-dev.snc1",    # your host, usually localhost
                     user="tracky",         # your username
                     passwd="bloodhound",  # your password
                     db="email")        # name of the data base

# you must create a Cursor object. It will let
#  you execute all the queries you need
cur = db.cursor()

# Use all the SQL you like
cur.execute("""SELECT * FROM email_meta""")

cur.execute("""INSERT INTO email_meta (date, hr, destination, country) VALUES ('2016-12-13', 12, 'sends', 'US')""")
db.commit()

# print all the first cell of all the rows
# for row in cur.fetchall():
#     print row[0]

db.close()