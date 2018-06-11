import time
from urllib.parse import urlparse
from html.parser import HTMLParser
from sqlalchemy.orm import Session
import random
import urllib.request
import threading
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Float, Text, create_engine, text as sa_text
from sqlalchemy.ext.declarative import declarative_base
MAXPROCESSED = 100
MAXUNPROCESSED = 100

NUM_STOPWORDS_IS_ENGLISH = 6
STOP_WORD_FILE = 'stop_words.txt'
Base = declarative_base()


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



class Webpage(HTMLParser,Base):
    stop_words = set()
    
    __tablename__ = 'webpage'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    domain = Column(String)
    text = Column(Text)
    stop_words = set(open(STOP_WORD_FILE,'r').read().split())
    
    def __init__(self, url ):
        HTMLParser.__init__(self)        
        self.words = []
        self.links = []
        
        self.url = url
        uri = urlparse(url)
        self.scheme = uri.scheme
        self.netloc = uri.netloc
        self.domain = '{uri.scheme}://{uri.netloc}'.format(uri = uri)
        
    def is_english(self):
        i=0
        for w in self.words:
            if w in self.stop_words:
                i += 1
                if i >= NUM_STOPWORDS_IS_ENGLISH:
                     return True
        return False
    
    def fill(self, timeout = 5):
        self.text = str(urllib.request.urlopen(url = self.url,timeout = timeout).read())
        
    def parse(self):
        self.feed(self.text)
        self.english = self.is_english()
    
    def get_link_pages(self):
        return list(map(Webpage,self.links))
    
    def handle_starttag(self,tag,attrs):
        if tag == 'a':
            for attrib in attrs:
                if attrib[0] == 'href':
                    link = urlparse(attrib[1])
                    if not (link.scheme and  link.netloc):
                        link = '{uri.scheme}://{uri.netloc}'.format(uri = urlparse(self.scheme + '://' +  self.netloc + attrib[1]))
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
            if not len(self):
                return None
            return super(Queue, self).pop(len(self)!=1 if random.randint(0,len(self)-1) else 0)

class RequestManager(threading.Thread):
    def __init__(self, unprocessed, processed, use_init = True, init_urls_file = 'init_urls.txt', lf='RequestManager.log', domain_once = False):
        super(RequestManager, self).__init__()
        self.logfile = open(lf,'a')
        self.errors = 0
        self.unprocessed = unprocessed
        if use_init:
            self.unprocessed+= map(Webpage,open(init_urls_file,'r').read().split())
        self.processed = processed
        self.closed_domains = set()
        self.closed_pages = set()
        self.do = domain_once
        self._stop_event = threading.Event()

    def run(self):
        i = 0
        while(not self._stop_event.is_set()):
            if len(self.processed) <= MAXPROCESSED and self.unprocessed:  
                page= self.unprocessed.randpop()
                try: 
                    page.fill()
                    self.processed.append(page)
                except Exception as e:
                    self.logfile.write('Request Manager exception: ' +str(e)+'\t' + time.strftime('%d/%m/%Y %H:%M:%S')+'\n')
                
                self.closed_domains.add(page.domain)
                self.closed_pages.add(page.url)
                if page.text:
                    print('Request Manager: crawled ' + page.url + ' page ' + str(i))
                    print('Request Manager: domain ' + page.domain)
                    i += 1
        self.close()

    def close(self):
        pass

    def stop(self):
        self._stop_event.set()


class Storage(threading.Thread):
    def __init__(self, processed, unprocessed, logfile='storage.log',dbfile = 'sqlite:///webpages.db', non_english = False):
        super(Storage, self).__init__()
        #\\C:\\Users\\Issiah\\Envs\\lsi_web_scraper\\lsi-web-crawler\\web_crawler\\webpages.db
        engine = create_engine(dbfile)
        self.engine = engine
        Base.metadata.create_all(engine)
        self.session = Session()
        self.logfile = open(logfile, 'a')
        self.processed = processed
        self.unprocessed = unprocessed
        self._stop_event = threading.Event()
        self.seen = set()

    def run(self):
        while(not self._stop_event.is_set()):
            if (len(self.processed) <= MAXUNPROCESSED) and self.unprocessed:
                page = self.unprocessed.randpop()
                page.parse()

                for linked_page in page.get_link_pages():
                    if linked_page not in self.seen:
                        self.processed.append(linked_page)
                        self.seen.add(linked_page)

                if page.english or non_english:
                    try:
                        self.session.add(page)
                        self.session.commit() 
                   
                    except Exception as e:
                        self.logfile.write('Storage Manager exception: ' + str(e) +'\t'+ time.strftime('%d/%m/%Y %H:%M:%S')+'\n')
                 
        self.close()

    def close(self):
        pass
    
    def stop(self):
        self._stop_event.set()

    
    def read_pages(self):
        return self.session.query(Webpage).all()

    def truncate_table(self):
        self.engine.execute(sa_text('''TRUNCATE TABLE webpage''').execution_options(autocommit=True))
'''
r = RequestManager()
s = Storage()
r.start()
s.start()
print("Starting crawling.")
r.join()
s.join()
'''
