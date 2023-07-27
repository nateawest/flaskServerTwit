import sqlite3
import json
import datetime
from datetime import datetime
from flask import Flask, request, jsonify
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

app = Flask(__name__)

# Connect to the SQLite database
def get_db_connection(path_name):
    conn = sqlite3.connect(path_name)
    conn.row_factory = sqlite3.Row

    return conn

# API endpoints

# Example: Get all data from a table in the database
@app.route('/api/data/<string:db_name>', methods=['GET'])
def get_data(db_name):
    path_name = '/home/ubuntu/' + db_name
    print("path name: " + path_name)
    conn = get_db_connection(path_name)
    cursor = conn.execute('SELECT * FROM data_table;')
    rows = cursor.fetchall()
    conn.close()

    # Convert Row objects to dictionaries
    data_list = [dict(row) for row in rows]

    return jsonify(data_list)


def tweet_exists(cursor, tweet):
    # Check if the tweet already exists in the data_table
    cursor.execute("SELECT COUNT(*) FROM data_table WHERE string_data = ?", (tweet,))
    count = cursor.fetchone()[0]
    return count > 0


# Example: Insert data into a table in the database
@app.route('/api/data/<string:db_name>', methods=['POST'])
def insert_data(db_name):
    path_name = '/home/ubuntu/' + db_name
    data = request.json
    tweet_list = json.loads(data)
    conn = get_db_connection(path_name)
    c = conn.cursor()
    # create an instance of the SentimentIntensity Analyster class from NLTK library
    sid = SentimentIntensityAnalyzer()
    date_without_time = datetime.today().strftime('%Y-%m-%d')
    # Process each tweet and store in the database
    for tweet in tweet_list:
        if not tweet_exists(c, tweet):  # let's make sure we aren't trying to store duplicates
            # Perform sentiment analysis
            sentiment_score = sid.polarity_scores(tweet)['compound']
            # Store the tweet, sentiment score and date in the data table
            c.execute("INSERT INTO data_table (string_data, sentiment_score, date_column) VALUES (?, ?, ?)",
                      (tweet, sentiment_score, date_without_time))
        else:
            print(f"Skipping tweet: '{tweet}' - Already exists in the database.")
    conn.commit()
    conn.close()

    return jsonify({'message': 'Data inserted successfully.'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)