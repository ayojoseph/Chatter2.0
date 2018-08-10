import sqlite3
import json
from datetime import datetime


# sys.setdefaultencoding('UTF8')
# reload(sys)
timeframe = '2015-05'
sql_transaction = []

connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

def createTable():
    c.execute("CREATE TABLE IF NOT EXISTS parent_reply (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")


def formatData(data):
    data = data.replace('\n',' newlinechar ').replace('\r',' newlinechar ').replace('"',"'")
    return data


def transactionBuilder(sql):
    global sql_transaction
    sql_transaction.append(sql)
    if len(sql_transaction) > 1000:
        c.execute('BEGIN TRANSACTION')
        for s in sql_transaction:
            try:
                cur.execute(s)
            except: 
                pass
        connection.commit()
        sql_transaction = []


def sqlInsertReplaceComment(commentId,parentId,parent,comment,subreddit,time,score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id = ?;""".format(parentId, commentId, parent, comment, subreddit, int(time), score, parentId)
        transactionBuilder(sql)
        
    except Exception as e:
        print('Replace Comment', str(e))

def sqlInsertHasParent(commentId,parentId,parent,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parentId, commentId, parent, comment, subreddit, int(time), score)
        transactionBuilder(sql)
        
    except Exception as e:
        print('Has Parent Comment', str(e))

def sqlInsertNoParent(commentId,parentId,comment,subreddit,time,score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parentId, commentId, comment, subreddit, int(time), score)
        transactionBuilder(sql)
        
    except Exception as e:
        print('No Parent COMMENT ERROR', str(e))

def acceptable(data):
    if len(data.split(' ')) > 50 or  len(data) < 1:
        return False
    elif len(data) > 1000:
        return False
    elif data == '[deleted]':
        return False 
    elif data == '[removed]':
        return False
    else:
        return True

def findParent(parId): 
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format(parId)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        return False

def findExistingScore(parent_id):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format(parent_id)
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False  
    except Exception as e:
        return False


if __name__ == '__main__':
    #print("Creating Table")
    createTable()
    rowCounter = 0
    pairedRows = 0 

    with open('./RC_{}'.format(timeframe.split('-')[0],timeframe), buffering=1000) as f:
        for row in f:
            #print(row)
            rowCounter+= 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = formatData(row['body'])
            createdUtc = row['created_utc']
            score = row['score']
            comment_id = row['name']
            subreddit = row['subreddit']
            parentData = findParent(parent_id)

            if score >= 2:
                existingCommentScore = findExistingScore(parent_id)
                if existingCommentScore:
                    if score > existingCommentScore:
                        if acceptable(body):
                            sqlInsertReplaceComment(comment_id, parent_id, parentData, body, subreddit, createdUtc, score)
                                                
                else:
                    if acceptable(body):
                        if parentData:
                            print("HERE")
                            sqlInsertHasParent(comment_id, parent_id, parentData, body, subreddit, createdUtc, score)
                            pairedRows += 1
                        else:
                            sqlInsertNoParent(comment_id, parent_id, body, subreddit, createdUtc, score)

            if rowCounter % 100000 == 0:
                print('Totals:  ROWS READ: {}, PAIRED ROWS: {}, TIME: {}'.format(rowCounter,pairedRows,str(datetime.now())))
                


