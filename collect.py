import datetime
import os
import re
from pymongo import MongoClient
from credentials import MONGOLAB_URL


DEFAULT_HARVEST_DATE = datetime.datetime(2016, 1, 1, 0, 0, 0)


def make_dir(dir):
    try:
        os.makedirs(dir)
    except OSError:
        if not os.path.isdir(dir):
            raise


def make_data_dir():
    data_dir = os.path.join(os.getcwd(), 'data')
    make_dir(data_dir)
    return data_dir


def get_db():
    dbclient = MongoClient(MONGOLAB_URL)
    db = dbclient.get_default_database()
    return db


def convert_harvest_date(harvest=None):
    try:
        dates = harvest.split('-')
        harvest_date = datetime.datetime(int(dates[0]), int(dates[1]), int(dates[2]), 0, 0, 0)
    except:
        harvest_date = DEFAULT_HARVEST_DATE
    return harvest_date


def get_reasons(harvest=None):
    harvest_date = convert_harvest_date(harvest)
    db = get_db()
    reasons = db.aggregates.find_one({'harvest_date': harvest_date, 'agg_type': 'reason_totals'})
    reason_list = [reason['reason'] for reason in reasons['results']]
    return reason_list


def get_titles(reason=None, series=None, year=None, clean=False):
    data_dir = make_data_dir()
    db = get_db()
    query = {}
    file_title = 'titles'
    if reason:
        query['reasons'] = reason
        file_title = '{}-{}'.format(file_title, reason.lower().replace(' ', '-'))
    if series:
        query['series'] = series
        file_title = '{}-{}'.format(file_title, series)
    if year:
        query['year'] = year
        file_title = '{}-{}'.format(file_title, year)
    if clean:
        file_title += '-cleaned'
    records = db.items.find(query).sort([['contents_dates.start_date.date', -1]])
    with open(os.path.join(data_dir, '{}.txt'.format(file_title)), 'wb') as titles:
        for record in records:
            if clean:
                title = re.sub(r'\[.+?\]', '', record['title'])
                title = re.sub(r'\s+', ' ', title).strip()
            else:
                title = record['title']
            titles.write('{}\n\n'.format(title))


def get_titles_by_reason(clean=True):
    reasons = get_reasons()
    for reason in reasons:
        get_titles(reason=reason, clean=clean)


