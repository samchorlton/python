import feedparser
import hashlib
from bs4 import BeautifulSoup
from urllib2 import urlopen
from HTMLParser import HTMLParser
from time import sleep
from elasticsearch import Elasticsearch

def get_article(section_url):
    html = urlopen(section_url).read()
    soup = BeautifulSoup(html, "lxml")
    articleContent = soup.find("span", {"id": "articleText"})
    return articleContent

class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

storedArticles = []

while 1:

    d = feedparser.parse('http://mf.feeds.reuters.com/reuters/UKWorldNews')

    print d['feed']['title']

    for article in d.entries:
        #print article['link']
        #print article['published']
        articleHash = hashlib.md5(article['link'] + article['published']).hexdigest()
        print str(articleHash)
        if articleHash not in storedArticles:
            storedArticles.append(articleHash)
            url = article['link']
            articleContents = strip_tags(str(get_article(url)))
            #print articleContents

    print len(storedArticles)

    sleep(60)
