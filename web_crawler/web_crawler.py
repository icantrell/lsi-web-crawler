import time
from urllib.parse import urlparse
from html.parser import HTMLParser
from sqlalchemy.orm import Session
import random
import urllib.request
import threading
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Float, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
MAXPROCESSED = 100
MAXUNPROCESSED = 100

NUM_STOPWORDS_IS_ENGLISH = 6
Base = declarative_base()

'''
class Word(Base):
    __tablename__ = 'word'
    id = Column(Integer, primary_key = True)
    word = Column(String)
    frequency = Column(Float)

class WordInstance(Base):
    __tablename__ = 'word_instance'
    id = Column(Integer, primary_key = True)
    frequency = Column(Float)
    webpage_id = Column(Integer, ForeignKey('webpage.id'))
    word_id = Column(Integer, ForeignKey('word.id'))
'''
class Webpage(HTMLParser,Base):
    stop_words = set()
    
    __tablename__ = 'webpage'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    domain = Column(String)
    text = Column(Text)
    stop_words = set(open(stop_word_file,'r').read().split())
    
    def __init__(self, url ):
        HTMLParser.__init__(self)        
        self.words = []
        self.links = []
        
        self.url = url
        uri = urlparse(url)
        self.scheme = uri.scheme
        self.netloc = uri.netloc
        self.domain = '{uri.scheme}://{uri.netloc}'.format(uri)
        
    def is_english(self):
        i=0
        for w in self.words:
            if w in self.stop_words:
                i += 1
                if i >= NUM_STOPWORDS_IS_ENGLISH:
                     return True
        return False
    
    def fill(self, timeout = 1):
        self.text = urllib.request.urlopen(url,timeout)
        
    def parse(self):
        self.feed(self.text)
        self.english = self.is_english()
    
    def get_link_pages():
        return [map(Webpage,self.links)]
    
    def handle_starttag(self,tag,attrs):
        if tag == 'a':
            for attrib in attrs:
                if attrib[0] == 'href':
                    link = urlparse(attrib[1])
                    if not (link.scheme and  link.netloc):
                        link = '{scheme}://{netloc}'.format(scheme = self.scheme, netloc = self.netloc + attrib[1]
                        self.links.append(link)
                else:
                    self.links.append(attrib[1])

    def handle_data(self, data):
        self.words += data.split()


class Queue(list):
    def __init__(self):
        self.lock = threading.RLock()
    
    def append(self, item):
        with self.lock:
            return super(Queue, self).append(item)
    
    def randpop(self):
        with self.lock:
            return super(Queue, self).pop(random.randint(0,len(self)-1))

class RequestManager(threading.Thread):
    def __init__(self, upqueue, pqueue, init_urls_file = 'init_urls.txt', lf='RequestManager.log', domain_once = False):
        super(RequestManager, self).__init__()
        self.logfile = open(lf,'a')
        self.errors = 0
        self.unprocessed = upqueue
        self.unprocessed+= map(Webpage,open(init_urls_file,'r').read().split())
        self.processed = pqueue
        self.closed_domains = set()
        self.closed_pages = set()
        self.do = domain_once

    def run(self):
        i = 0
        while(1):
            if len(self.processed) <= MAXPROCESSED: 
                page= self.unprocessed.randpop()
                try: 
                    page.fill()
                    self.processed.append(page)
                except Exception as e:
                    self.logfile.write('Request Manager exception: ' +str(e)+'\t' + time.strftime('%d/%m/%Y %H:%M:%S')+'\n')
                
                self.closed_domains.add(page.domain)
                self.closed_pages.add(page.url)
                print('crawled ' + page.url + ' page ' + str(i))
                print('domain ' + page.domain)
                i += 1
        self.close()

    def close(self):
        pass


    def queue_page(self, webpage_url):
        if not (self.do and webpage.domain in self.closed_domains) and (webpage.url not in self.closed_pages):
            self.unprocessed.append(webpage)

    def unprocessed_count(self):
        return len(self.unprocessed)
    
    def pop_processed(self):
        return self.processed.randpop()

class Storage(threading.Thread):
    def __init__(self, RM, logfile='storage.log'):
        super(Storage, self).__init__()
        self.RM = RM
        #\\C:\\Users\\Issiah\\Envs\\lsi_web_scraper\\lsi-web-crawler\\web_crawler\\webpages.db
        engine = create_engine('sqlite:///webpages.db')
        Base.metadata.create_all(engine)
        self.session = Session()
        self.logfile = open(logfile, 'a')

    def run(self):
        while(1):
            if (len(self.RM.unprocessed) <= MAXUNPROCESSED) and len(self.RM.processed):
                page = self.RM.pop_processed()
                page.parse()

                for link in page.links():
                    RM.queue_page(link)

                if page.english:
                    try:
                        self.session.add(page)
                        self.session.commit() 
                   
                    except Exception as e:
                        self.logfile.write('Storage Manager exception: ' + str(e) +'\t'+ time.strftime('%d/%m/%Y %H:%M:%S')+'\n')
        
        self.close()

    def close(self):
        pass

r = RequestManager()
s = Storage(r)
r.start()
s.start()
print("Starting crawling.")
r.join()
s.join()
