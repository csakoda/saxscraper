from datetime import datetime
from dateutil.parser import parse
from lxml import html
import requests
import argparse
import logging
import sqlite3

log = logging.getLogger(__name__)
logging.getLogger("requests").setLevel(logging.WARNING) # disable noise

conn = sqlite3.connect('scrape.db')
db = conn.cursor()

parser = argparse.ArgumentParser(description='Scrape data from phish.net')
parser.add_argument('--all-years', action='store_true',
                    help='Scrape all years')
parser.add_argument('--year', action='store',
                    help='Scrape individual year')
parser.add_argument('--debug', action='store_true',
                    help='Enable debug output')

args = parser.parse_args()

# todo make this dynamic
phish_years = [ '1983', '1984', '1985', '1986', '1987', '1988', '1989', '1990', '1991', 
                '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000', 
                '2001', '2002', '2003', '2004', '2008', '2009', '2010', '2011', '2012', 
                '2013', '2014' ]

if args.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

db.execute('CREATE TABLE IF NOT EXISTS shows (id numeric, date numeric, rating real, votes integer)')
conn.commit()

def scrape_date(date):
    log.debug('Scraping date {0}'.format(date))
    if parse(date) > datetime.now():
        log.debug('Skipping future show {0}'.format(date))
        return
    scrape_rating(date)
    
# Input: [ ' 3.261' ]
# Output: 3.261
def parse_rating(rating):
    return rating[0].strip()

# Input: ['Rating: ', '/5 (159 votes cast)']
# Output: 159
def parse_votes(votes):
    return votes[1].split(' ')[1][1:]    
    
def scrape_rating(date, id=None):
    if not id:
        id = date
        url = 'http://phish.net/setlists/?d={0}'.format(date)
    else:
        url = 'http://phish.net/setlists/?showid={0}'.format(id)
    db.execute('select * from shows where id=?', (id,))
    result = db.fetchone()
    if not result:
        page = requests.get(url)
        log.debug('Scraped {0}'.format(url))
        tree = html.fromstring(page.text)
        rating = tree.xpath('//span[@id="ratingsection"]/span[@style="margin-left:20px;"]/strong/text()')
        if len(rating) == 0:
            # there must have been multiple shows on this day
            shows = tree.xpath('//a[contains(@href, "/setlists/?showid=")]/@href')
            for show in shows:
                scrape_rating(date, id=show[18:])
        else:
            log.debug('Date: {0}'.format(date))
            rating = parse_rating(rating)
            log.debug('Rating: {0}'.format(rating))
            votes = parse_votes(tree.xpath('//span[@id="ratingsection"]/span[@style="margin-left:20px;"]/text()'))
            log.debug('Votes: {0}'.format(votes))
            db.execute('insert into shows values (?, ?, ?, ?)', (id, date, rating, votes))
            conn.commit()
            log.info('Scraped {0} ({1})'.format(date, rating))
    else:
        if result[0] != result[1]:
            log.info('Already scraped {0} [{1}] ({2})'.format(result[1], result[0], result[2]))
        else:
            log.info('Already scraped {0} ({1})'.format(result[1], result[2]))

def scrape_year(year):
    log.debug('Scraping year {0}'.format(year))
    page = requests.get('http://phish.net/setlists/{0}.html'.format(year))
    tree = html.fromstring(page.text)
    dates = tree.xpath('//div[@class="setlist"]/h2/a[contains(@href,"http://phish.net/setlists/?d=")]/@href')
    for url in (set(dates)):
        date = url[29:]
        scrape_date(date)

if args.all_years:
    for year in phish_years:
        scrape_year(year)
    
if args.year:
    scrape_year(args.year)
    
