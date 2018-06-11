from web_crawler import *
import time
    

def test_webpage_not_english():
    page = Webpage('http://primes.utm.edu/lists/small/10000.txt')

    page.fill()
    page.parse()

    assert(not page.english)

def test_Queue():
    q = Queue()
    assert(q.randpop() == None)
    q.append(1)
    assert(q.randpop() == 1)
    q.randpop()
    assert(q.randpop() == None)
    q.append(1)
    q.append(2)
    assert(q.randpop() in [1,2])

def test_RM():
    p = Queue()
    up = Queue()

    rm = RequestManager(up,p, use_init = False)
    try:
        rm.start()

        up.append(Webpage('https://en.wikipedia.org/wiki/Singular-value_decomposition'))
        time.sleep(3)
        page = p.randpop()
        assert(page)
        page.fill()
        page.parse()
        assert(page.text)
        if(page.english):
            assert(page.links)
        assert(rm.isAlive())
        if(page.links):
            assert(page.english)
        rm.stop()
        time.sleep(2)
        assert(not rm.isAlive())
    finally:
        rm.stop()
def test_Storage():
    po = Queue()
    up = Queue()

    s = Storage(po,up,dbfile='sqlite:///test_webpages.db')
    try:
        s.start()
        page = Webpage('https://en.wikipedia.org/wiki/Singular-value_decomposition')
        page.fill()
        up.append(page)
        time.sleep(2)
        assert(len(po))
        i = 0
        for p in po:
            if p.scheme and p.netloc:
                i += 1
        assert(i)
        assert(s.isAlive())
        s.stop()
        time.sleep(2)
        assert(not s.isAlive())
    finally:
        s.stop()

def test_stop():
    rm = RequestManager(Queue(),Queue(),use_init = False)
    s = Storage(Queue(),Queue())
    try:
        rm.start()
        s.start()
        assert(rm.isAlive())
        assert(s.isAlive())
        rm.stop()
        s.stop()
        time.sleep(3)
        assert(not rm.isAlive())

        assert(not s.isAlive())
    finally:
        s.stop()
        rm.stop()
def test_select_query():
    try:
        s = Storage(Queue(), Queue(),dbfile='sqlite:///test_webpages.db')
        s.truncate_table()
        r = s.read_pages()
        assert(r)
        print(r)
    finally:
        s.stop()
