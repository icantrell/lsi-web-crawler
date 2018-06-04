import random
import urllib
import threading
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Float, Text
from sqlalchemy.ext.declarative import declarative_base


Base = declaritive_base()


class Word:
    __tablename__ = 'word'
    id = Column(Integer, primary_key = True)
    word = Column(String)
    frequency = Column(Float)

class WordInstance:
    __tablename__ = 'word_instance'
    id = Column(Integer, primary_key = True)
    frequency = Column(Float)
    webpage_id = Column(Integer, ForeignKey('webpage.id')
    word_id = Column(Integer, ForeignKey('word.id')

class Webpage:
    stop_words = []
    __tablename__ = 'webpage'
    id = Column(Integer, primary_key=True)
    url = Column(String)
    domain = Column(String)
    text = Colum(Text)
    
    def set_page(page_text):
        self.text = page_text
    def get_frequencies():
       pass 
        
class Queue(list):
    def __init__(self):
        self.lock = threading.Rlock()
    
    def append(self, item):
        with self.lock:
            return super.append(item)
    
    def randpop(self):
        with self.lock:
            return super.pop(random.randint(0,len(super)-1)

class RequestManager(threading.Thread):
    def __init__(self, lf, upqueue, pqueue):
        super(RequestManager, self).__init__()
        self.logfile = open(lf,'a')
        self.errors = 0
        self.unprocessed = upqueue
        self.processed = pqueue

    def run(self):
        while(self.unprocessed):
            if len(self.processed) <= MAXPROCESSED: 
                page= self.unprocessed.randpop()
                try: 
                    text = urllib.request.urlopen(page.get_url(),timeout = 1).read()
                    page.set_text(text)
                    self.processed.append(page)
                except urllib2.HTTPError, e:
                    self.logfile.write('HTTPError = ' + str(e.code)+'\n')
                except urllib2.URLError, e:
                    self.logfile.write('URLError = ' + str(e.reason)+'\n')
                except httplib.HTTPException, e:
                    self.logfile.write('HTTPException'+str(e.reason)+'\n')
                except Exception:
                    import traceback
                    self.logfile.write(traceback.format_exc()+'\n')

        self.close()

    def close(self):
        pass


    def queue_page(self, webpage):
        self.unprocessed.append(webpage)

    def unprocessed_count(self):
        return len(self.unprocessed)


class storage(threading.Thread):
    def __init__(self, RM):
        super(Parser, self).__init__()
        self.RM = RM

    def run(self):
        while(RM.isalive()):
            if len(RM.unprocessed.count() <= MAXUNPROCESSED) and RM.processed_count():
                page = RM.pop_processed()
                page.set_frequencies()
                try:
                    for w in page.get_words():
                        if w not in self.rec_words:
                            word = self.session.query(Word).filter_by(word = w)
                            if not word:
                                word = Word(word = w)
                                self.session.add(word)
                            wid = word.id

                        else:
                            wid = self.rec_words[w]

                                 
