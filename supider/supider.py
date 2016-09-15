from bs4 import BeautifulSoup
import time
import traceback

import connector
from utils import Pool
from utils import timeit
from utils import TreeBar


"""
TODO:
    1. Test mode
"""
concrete_cost = 0


class SupiderError(Exception):
    pass
class SupiderFatalError(SupiderError):
    pass
class SupiderFailure(SupiderError):
    pass
class SupiderStop(SupiderError):
    pass


class Spider(object):
    def __init__(self, f_crawl, target_webs):
        self.crawl = f_crawl

        target_type = type(target_webs)
        if target_type is not list:
            if target_type is Cobweb:
                target_webs = [target_webs]
            else:
                raise SupiderFatalError(
                    "Spider %s expect cobweb object or list, but actually %s"
                    % (self.name, target_type))
        self.cobwebs = target_webs

    @property
    def name(self):
        return self.crawl.func_name

    def __call__(self, soup, item, supider):
        global concrete_cost
        ts = time.time()
        results = self.crawl(soup, item)
        te = time.time()
        concrete_cost += (te-ts)

        # Process cobwebs
        web_size = len(self.cobwebs)
        if web_size > 1:
            if len(results) != web_size:
                raise SupiderFatalError(
                    "Spider %s error: The number of cobwebs %s != return value length %s!"
                    % (self.name, web_size, len(results)))
            else:
                for i in range(web_size):
                    self.cobwebs[i].extend(results[i])
                for i in range(web_size):
                    self.cobwebs[i].process_items(supider, results[i])
        elif web_size == 1:
            self.cobwebs[0].extend(results)
            self.cobwebs[0].process_items(supider, results)
        else:
            raise SupiderFatalError(
                    "Spider %s error: It has no cobweb to process!" % self.name)

        return results


class Cobweb(object):
    def __init__(self, name, item_get_url,
                 consumable, items, tree_item):
        self.name = name
        self.spiders = []
        self.item_get_url = item_get_url
        self.consumable = consumable
        self._tree_item = tree_item

        self.ready = []
        self.items = []
        self.success = []

        self.ready.extend(items)

    def crawl_next(self, f_crawl, webs):
        if self.consumable:
            spider = Spider(f_crawl, webs)
            self.spiders.append(spider)
        else:
            raise SupiderFatalError("Cobweb %s is not consumable!" % self.name)

    def start(self, supider):
        if self.ready:
            proceed = self.ready
            self.ready = []
            try:
                self.process_items(supider, proceed)
            except SupiderStop:
                self.ready.extend(proceed)
                raise

    def process_items(self, supider, items):
        if self.consumable:
            def protected_func(item):
                try:
                    ret = self._process_item(item, supider)
                    return ret
                except (SupiderFailure, SupiderStop) as e:
                    self.ready.append(item)
                    raise
            supider.poolify(items, protected_func)

    def _process_item(self, item, supider):
        url = self.item_get_url(item)
        if type(url) not in (str, unicode):
            raise SupiderFatalError(
                "URL type is not correct, expected str unicode, but %s"
                % type(url))
        soup = supider.process_url(url)
        for spider in self.spiders:
            spider(soup, item, supider)
        self._consume(item)

    def _consume(self, item):
        self.success.append(item)
        self._tree_item.value += 1

    def extend(self, items):
        self.items.extend(items)
        self._tree_item.size += len(items)


class ErrorMsg(object):
    def __init__(self, level, place, message):
        self.level = level
        self.place = place
        self.message = message

    def __str__(self):
        return ">>> %s, %s:\n%s"\
               % (self.level, self.place, self.message)


class ErrorCollector(object):
    def __init__(self):
        self._errors = []
        self._fatals = []

    def __bool__(self):
        return bool(self._errors or self._fatals)
    __nonzero__ = __bool__

    @property
    def fail(self):
        return bool(self._fatals)

    def reset(self):
        self._errors = []
        self._fatals= []

    def add(self, e, item):
        if isinstance(e, SupiderFailure):
            error = ErrorMsg("FAIL", item, e.message)
            self._errors.append(error)
        elif isinstance(e, SupiderFatalError):
            error = ErrorMsg("FATAL", item, e.message)
            self._fatals.append(error)
        elif isinstance(e, SupiderStop):
            pass
        else:
            msg = str(type(e)) + "\n"
            msg += traceback.format_exc()
            error = ErrorMsg("FATAL", item, msg)
            self._fatals.append(error)

    def __str__(self):
        if self._fatals:
            return "\n".join(str(err) for err in self._fatals)
        elif self._errors:
            return "\n".join(str(err) for err in self._errors)
        else:
            return "No errors"


class Supider(object):
    def __init__(self, base_url=None, concurrency=5,
                 codec="UTF-8", f_check_soup=None):
        self._base_url = base_url
        self._codec = codec
        self._f_check_soup = f_check_soup
        self._webs = []

        self._tree_bar = TreeBar()
        self.concurrency = concurrency
        self._pool = Pool(concurrency*2)
        self._connector = connector.Connector(concurrency=concurrency)
        self.error = ErrorCollector()

    def __bool__(self):
        return not self.error
    __nonzero__ = __bool__

    def poolify(self, iterable, func, serial=False):
        if self.error:
            raise SupiderStop()

        def protected_func(item):
            ret = None
            try:
                ret = func(item)
            except Exception as e:
                self.error.add(e, item)
            return ret

        return self._pool.poolify(iterable, protected_func, serial)

    def process_url(self, url):
        if self.error:
            raise SupiderStop()

        if self._base_url is not None:
            url = self._base_url + url

        source_code = self._connector.connect(url)
        if isinstance(source_code, connector.Fail):
            if not source_code.message:
                raise SupiderStop()
            else:
                raise SupiderFailure("Fail to connect %s: %s"
                                     % (url, source_code.message))
        if isinstance(source_code, connector.Error):
            raise SupiderFatalError("Error to connect %s:\n%s"
                                    % (url, source_code.message))

        ts = time.time()
        unicode_text = source_code.decode(self._codec)
        # default "html.parser"
        soup = BeautifulSoup(unicode_text, "lxml")
        te = time.time()
        global concrete_cost
        concrete_cost += (te-ts)

        if self._f_check_soup and self._f_check_soup(soup):
            raise SupiderFailure(url)
        return soup

    def register_web(self, name,
                     item_get_url=lambda a: a,
                     consumable=True,
                     items=None):
        if not items:
            items = []
        elif type(items) is not list:
            raise RuntimeError(
                "Cobweb %s expect items of list, but actually %s"
                % (name, type(items)))
        tree_item = self._tree_bar.create_item(name,
                                               consumable=consumable,
                                               size=len(items))
        web = Cobweb(name, item_get_url, consumable, items, tree_item)
        self._webs.append(web)
        return web

    def reset_state(self, pool_size):
        self.concurrency = pool_size
        self._connector.reset(pool_size)
        self._pool.reset(pool_size*2)

        self.error.reset()
        self._tree_bar.reset()

    @timeit
    def crawl(self):
        splitter = "-"*60
        while True:
            print splitter
            print " "*20 + "Supider start (%s):" % self.concurrency
            print

            try:
                for web in self._webs:
                    web.start(self)
            except SupiderStop:
                pass
            print

            print splitter
            print " "*20 + "Run report:"
            print
            print self.error
            print

            if self.error.fail or not self.error:
                break


            for web in self._webs:
                if web.ready:
                    print "%s has %s items pending" % (web.name, len(web.ready))
            print

            raw_con = raw_input("Enter concurrency to continue (1): ")
            try:
                conc = int(raw_con)
            except Exception:
                conc = 1
            else:
                if conc < 1:
                    conc = 1
            print

            self.reset_state(conc)
        print "Concrete cost: %2.5f sec" % concrete_cost
