import datetime
import json
import os
import sqlite3

import requests
from requests_oauthlib import OAuth1Session

import credentials


def download_follower_data(datestamp):
    log('Downloading follower data for {}'.format(datestamp))
    base_path = os.path.join('data', 'raw', datestamp)

    if os.path.exists(base_path):
        log('Data already downloaded')
        return

    os.makedirs(base_path, exist_ok=True)

    session = OAuth1Session(
        client_key=credentials.consumer_key,
        client_secret=credentials.consumer_secret,
        resource_owner_key=credentials.access_token,
        resource_owner_secret=credentials.access_token_secret,
    )

    url = 'https://api.twitter.com/1.1/followers/list.json'
    cursor = -1
    page = 0

    while cursor != 0:
        log('Downloading page {}'.format(page))
        params = {
            'count': 200,
            'skip_status': True,
            'include_user_entities': False,
            'cursor': cursor,
        }
        rsp = session.get(url, params=params)
        data = rsp.json()

        path = os.path.join(base_path, 'rsp-{}.json'.format(page))
        log('Saving data to {}'.format(path))
        
        with open(path, 'w') as f:
            json.dump(data, f, indent=4, sort_keys=True)

        cursor = data['next_cursor']
        page += 1

    log('Downloaded follower data to {}'.format(base_path))


def extract_follower_handles(datestamp):
    log('Extracting follower data for {}'.format(datestamp))
    path = os.path.join('data', 'followers', '{}.txt'.format(datestamp))

    if os.path.exists(path):
        log('Followers already extracted')
        return

    os.makedirs(os.path.join('data', 'followers'), exist_ok=True)

    handles = []
    
    raw_data_path = os.path.join('data', 'raw', datestamp)
    for filename in os.listdir(raw_data_path):
        log('Extracting follower data from {}'.format(filename))
        with open(os.path.join(raw_data_path, filename)) as f:
            data = json.load(f)

        for record in data['users']:
            handles.append(record['screen_name'])

    log('Extracted {} follower handles'.format(len(handles)))

    with open(path, 'w') as f:
        for handle in sorted(handles):
            f.write(handle + '\n')

    log('Extracted handles to {}'.format(path))


def update_follower_db(datestamp):
    log('Updating follower database for {}'.format(datestamp))
    db_path = os.path.join('data', 'twitter-followers.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('CREATE TABLE IF NOT EXISTS runs (datestamp text primary key)')
    cursor.execute('CREATE TABLE IF NOT EXISTS followers (handle text primary key, firstseen text, lastseen text)')

    cursor.execute('SELECT COUNT(*) FROM runs WHERE datestamp = ?', [datestamp])
    result = cursor.fetchone()
    if result[0] == 1:
        log('Follower database already has data from {}'.format(datestamp))
        return

    cursor.execute('SELECT COUNT(*) FROM runs WHERE datestamp > ?', [datestamp])
    result = cursor.fetchone()
    if result[0] > 0:
        log_error_and_exit('Follower database has data from after {}'.format(datestamp))

    followers_path = os.path.join('data', 'followers', '{}.txt'.format(datestamp))
    handles = []
    with open(followers_path) as f:
        handles = [line.strip() for line in f]

    for handle in handles:
        cursor.execute('SELECT COUNT(*) FROM followers WHERE handle = ?', [handle])
        result = cursor.fetchone()

        if result[0] == 1:
            cursor.execute(
                'UPDATE followers SET lastseen = ? WHERE handle = ?',
                [datestamp, handle]
            )
        else:
            cursor.execute(
                'INSERT INTO followers (handle, firstseen, lastseen) VALUES (?, ?, ?)',
                [handle, datestamp, datestamp]
            )

    cursor.execute('INSERT INTO runs (datestamp) VALUES (?)', [datestamp])
    conn.commit()

    log('Updated follower database')


def produce_daily_summary(datestamp):
    log('Producing a summary report for {}'.format(datestamp))

    report_path = os.path.join('reports', '{}.txt'.format(datestamp))
    os.makedirs('reports', exist_ok=True)
    if os.path.exists(report_path):
        log('Report already generated for {}'.format(datestamp))

    date = datetime.datetime.strptime(datestamp, '%Y-%m-%d').date()
    previous_date = date - datetime.timedelta(days=1)
    previous_datestamp = previous_date.strftime('%Y-%m-%d')

    db_path = os.path.join('data', 'twitter-followers.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('SELECT COUNT(*) FROM runs WHERE datestamp = ?', [datestamp])
    result = cursor.fetchone()
    if result[0] == 0:
        log_error_and_exit('Follower database has no data from {}'.format(datestamp))

    cursor.execute('SELECT COUNT(*) FROM runs WHERE datestamp = ?', [previous_datestamp])
    result = cursor.fetchone()
    if result[0] == 0:
        log_error_and_exit('Follower database has no data from {}'.format(previous_datestamp))

    cursor.execute(
        'SELECT COUNT(*) FROM followers WHERE firstseen <= ? AND lastseen >= ?',
        [datestamp, datestamp]
    )
    result = cursor.fetchone()
    num_followers = result[0]
    
    cursor.execute(
        'SELECT handle FROM followers WHERE firstseen = ?',
        [datestamp]
    )
    results = cursor.fetchall()
    new_followers = [result[0] for result in results]
    
    cursor.execute(
        'SELECT handle FROM followers WHERE lastseen = ?',
        [previous_datestamp]
    )
    results = cursor.fetchall()
    ex_followers = [result[0] for result in results]

    with open(report_path, 'w') as f:
        f.write('Twitter follower report for {}\n\n'.format(datestamp))
        f.write('You have {} followers\n\n'.format(num_followers))

        if new_followers:
            f.write('{} new follower(s):\n'.format(len(new_followers)))
            for follower in new_followers:
                f.write(' * {}\n'.format(follower))
        else:
            f.write('No new follower(s):\n')

        f.write('\n')

        if ex_followers:
            f.write('{} new ex follower(s):\n'.format(len(new_followers)))
            for follower in ex_followers:
                f.write(' * {}\n'.format(follower))
        else:
            f.write('No new ex follower(s):\n')

    log('Produced summary report in {}'.format(report_path))


def log(msg):
    print(msg)


def log_error_and_exit(msg):
    print('=' * 80)
    print('WARNING: {}'.format(msg))
    print('=' * 80)
    exit(1)

    
if __name__ == '__main__':
    datestamp = datetime.date.today().strftime('%Y-%m-%d')

    download_follower_data(datestamp)
    extract_follower_handles(datestamp)
    update_follower_db(datestamp)
    produce_daily_summary(datestamp)
