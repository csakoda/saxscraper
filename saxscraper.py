from datetime import datetime
from dateutil.parser import parse
from lxml import html
import requests
import argparse
import logging as log
import sqlite3
conn = sqlite3.connect('scrape.db')
db = conn.cursor()

parser = argparse.ArgumentParser(description='Scrape data from phish.net')
parser.add_argument('--all-years', action='store_true',
                    help='Show rating of a show')
parser.add_argument('--debug', action='store_true',
                    help='Enable debug output')

args = parser.parse_args()

# todo make this dynamic
phish_years = [ '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', 
                '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', 
                '2001', '2002', '2003', '2004', '2008', '2009', '2010', '2011', '2012', 
                '2013', '2014' ]

if args.debug:
    log.basicConfig(level=log.DEBUG)

db.execute('CREATE TABLE IF NOT EXISTS shows (id numeric, date numeric, rating real, votes integer)')
conn.commit()

def scrape_date(date):
    if parse(date) > datetime.now():
        log.debug('Skipping future show {0}'.format(date))
        return

    url = 'http://phish.net/setlists/?d={0}'.format(date)
    page = requests.get(url)
    log.debug('Scraped {0}'.format(url))
    tree = html.fromstring(page.text)
    scrape_rating(date, tree)
    

# like 'Currently 3.26/5'
def parse_rating(rating):
    return rating[0].split(' ')[1].split('/')[0]
    
def scrape_rating(date, tree, id=None):
    if not id:
        id = date

    db.execute('select * from shows where id=?', (id,))
    result = db.fetchone()
    if not result:
        rating = tree.xpath('//span[@id="ratingsection"]/span[@style="margin-left:20px;"]/strong/text()')
        if len(rating) == 0:
            # there must have been multiple shows on this day
            shows = tree.xpath('//a[contains(@href, "/setlists/?showid=")]/@href')
            for show in shows:
                page = requests.get('http://phish.net{0}'.format(show))
                scrape_rating(date, html.fromstring(page.text), id=show[18:])
        else:
            rating = parse_rating(rating)
            log.debug('Rating: {0}'.format(rating))
            votes = tree.xpath('//span[@id="ratingsection"]/span[@style="margin-left:20px;"]/text()')[1].split(' ')[1][1:]
            log.debug('Votes: {0}'.format(votes))
            log.debug('Date: {0}'.format(date))
            db.execute('insert into shows values (?, ?, ?, ?)', (id, date, rating, votes))
            conn.commit()
            log.info('Scraped {0} ({1})'.format(date, rating))
    else:
        log.info('Already scraped {0} ({1})'.format(result[0], result[1]))

if args.all_years:
    for year in phish_years:
        page = requests.get('http://phish.net/setlists/{0}.html'.format(year))
        tree = html.fromstring(page.text)
        dates = tree.xpath('//a[contains(@href,"http://phish.net/setlists/?d=")]/@href')
        for url in (set(dates)):
            date = url[29:]
            scrape_date(date)

    
