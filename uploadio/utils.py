import inspect
import logging
import os
from hashlib import md5


def make_md5(s, encoding='utf-8'):
    return md5(s.encode(encoding)).hexdigest()


def make_list(item_or_items):
    """
        Makes a list out of the given items.
        Examples:
            >>> make_list(1)
            [1]
            >>> make_list('str')
            ['str']
            >>> make_list(('i', 'am', 'a', 'tuple'))
            ['i', 'am', 'a', 'tuple']
            >>> print(make_list(None))
            None
            >>> # An instance of lists is unchanged
            >>> l = ['i', 'am', 'a', 'list']
            >>> l_res = make_list(l)
            >>> l_res
            ['i', 'am', 'a', 'list']
            >>> l_res is l
            True

        Args:
            item_or_items: A single value or an iterable.
        Returns:
            Returns the given argument as an list.
        """
    if item_or_items is None:
        return None
    if isinstance(item_or_items, list):
        return item_or_items
    if isinstance(item_or_items, dict):
        return [item_or_items]
    if hasattr(item_or_items, '__iter__') \
            and not isinstance(item_or_items, str):
        return list(item_or_items)
    return [item_or_items]


def get_field_mro(cls, field_name):
    res = set()
    for c in inspect.getmro(cls):
        values_ = getattr(c, field_name, None)
        if values_ is not None:
            res = res.union(set(make_list(values_)))
    return res


def auto_str(__repr__=False):
    """
    Use this decorator to auto implement __str__()
    and optionally __repr__() methods on classes.
    Args:
        __repr__ (bool): If set to true, the decorator will auto-implement
        the __repr__() method as well.
    Returns:
        callable: Decorating function.
    Note:
        There are known issues with self referencing (self.s = self).
        Recursion will be identified by the python
        interpreter and will do no harm, but it will actually not work.
        A eval(class.__repr__()) will obviously not work, when there are
        attributes that are not part of the
        __init__'s arguments.
    Example:
        >>> @auto_str(__repr__=True)
        ... class Demo(object):
        ...    def __init__(self, i=0, s="a", l=None, d=None):
        ...        self.i = i
        ...        self.s = s
        ...        self.l = l
        ...        self.d = d
        >>> dut = Demo(10, 'abc', [1, 2, 3], {'a': 1, 'b': 2})
        >>> print(dut.__str__())
        Demo(i=10, s='abc', l=[1, 2, 3], d={'a': 1, 'b': 2})
        >>> print(eval(dut.__repr__()).__str__())
        Demo(i=10, s='abc', l=[1, 2, 3], d={'a': 1, 'b': 2})
        >>> print(dut.__repr__())
        Demo(i=10, s='abc', l=[1, 2, 3], d={'a': 1, 'b': 2})
    """
    def decorator(cls):
        def __str__(self):
            items = ["{name}={value}".format(
                name=name,
                value=value.__repr__()
            ) for name, value in vars(self).items()
                if name not in get_field_mro(self.__class__,
                                             '__auto_str_ignore__')]
            return "{clazz}({items})".format(
                clazz=str(type(self).__name__),
                items=', '.join(items)
            )
        cls.__str__ = __str__
        if __repr__:
            cls.__repr__ = __str__

        return cls

    return decorator


class Loggable:

    """
    Adds a logger property to the class to provide easy access to a
    configured logging instance to use.
    Example:
        >>> class NeedsLogger(Loggable):
        ...     def do(self, message):
        ...         self.logger.info(message)
    """
    @property
    def logger(self) -> logging.Logger:
        """
        Configures and returns a logger instance for further use.
        Returns:
            (logging.Logger)
        """
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(name)-15s - [%(levelname)-10s] %(message)s"
        )
        return logging.getLogger(os.path.basename(__file__))
