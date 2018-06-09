import web_crawler

def test_webpage():
    page = Webpage('https://en.wikipedia.org/wiki/Association_rule_learning#Algorithms')
    assert(page.scheme)
    assert(page.netloc)

    page.fill()

    page.parse()

    for p in page.get_link_pages():
        assert(page.scheme)
        assert(page.netloc)
        p.fill()

    assert(page.english)

def test_webpage_not_english():
    page = Webpage('')

    page.fill()

    assert(not page.english)

def test_RM():
    p = Queue()
    up = Queue()

    rm = RequestManager()

    rm.start()

    rm.unprocessed.append(Webpage(''))
    rm.processed
