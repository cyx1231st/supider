import eventlet
eventlet.monkey_patch()

from blessings import Terminal
from progressive import bar
from progressive.tree import ProgressTree, Value, BarDescriptor
import time


class Bar(object):
    def __init__(self, title, max_len):
        self._bar = bar.Bar(max_value=max_len)
        self._max_len = max_len
        self._current = 0

        print title + ":"
        self._bar.cursor.clear_lines(2)
        self._bar.cursor.save()
        self.trigger()

    def trigger(self, step=1):
        if self._current > self._max_len:
            raise RuntimeError("Exceed max length %s" % self._max_len)
        self._bar.cursor.restore()
        self._bar.draw(value=self._current)
        self._current += step


_EMPTY_DES = BarDescriptor(
        value=Value(0),
        type=bar.Bar,
        kwargs=dict(width="0.1%", num_rep="percentage"))


class _BarItem(object):
    def __init__(self, name, key, parent, size=0, value=0, consumable=True):
        self._name = name
        self.key = key
        self._parent = parent
        if consumable:
            des = BarDescriptor(
                value=Value(value),
                type=bar.Bar,
                kwargs=dict(max_value=size))
        else:
            des = BarDescriptor(
                value=Value(value),
                type=bar.Bar,
                kwargs=dict(max_value=size, width="0.1%"))
        self._consumable = consumable
        self._bar_descriptor = des

    def _refresh_parent(self):
        self._parent._update_desc(self)
        self._parent.draw()

    @property
    def size(self):
        return self._bar_descriptor.get("kwargs")["max_value"]

    @size.setter
    def size(self, value):
        self._bar_descriptor.get("kwargs")["max_value"] = value
        self._refresh_parent()

    @property
    def value(self):
        return self._bar_descriptor.get("value").value

    @value.setter
    def value(self, value):
        if not self._consumable:
            raise RuntimeError("%s is not consumable!" % self.name)
        self._bar_descriptor.get("value").value = value
        self._refresh_parent()

    @property
    def bar_descriptor(self):
        if self.size == 0:
            return _EMPTY_DES
        else:
            return self._bar_descriptor


class TreeBar(object):
    def __init__(self, disabled=False):
        self._progress_tree = ProgressTree(term=Terminal())
        self._descriptors = {}
        self._draw = False
        self._index = 0
        self._disabled = disabled

    def _update_desc(self, item):
        self._descriptors[item.key] = item.bar_descriptor

    def create_item(self, name, size=0, progress=0, consumable=True):
        if self._draw:
            raise RuntimeError(
                "TreeBar already drawing, cannot create new item %s" % name)
        self._index += 1
        key = "%s) %s" % (self._index, name)
        bar_item = _BarItem(name, key, self, size, progress, consumable)
        self._update_desc(bar_item)
        return bar_item

    def _ready(self):
        self._progress_tree.make_room(self._descriptors)
        self._progress_tree.cursor.save()
        self._draw = True

    def reset(self):
        self._draw = False

    def draw(self, redraw=False):
        if self._disabled:
            return
        if not self._draw or redraw:
            self._ready()
        self._progress_tree.cursor.restore()
        self._progress_tree.draw(self._descriptors)


class Pool(object):
    def __init__(self, size):
        self._pool = eventlet.GreenPool(size)

    def reset(self, size):
        self._pool.resize(size)

    def poolify(self, iterable, func, serial=False):
        results = []
        if not iterable:
            return results

        if not serial:
            pile = eventlet.GreenPile(self._pool)
            for it in iterable:
                pile.spawn(func, it)
            for ret in pile:
                results.append(ret)
        else:
            for it in iterable:
                ret = func(it)
                results.append(ret)

        return results


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r costs %2.5f sec' % (method.__name__, te-ts)
        return result

    return timed
