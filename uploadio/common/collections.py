from collections import defaultdict
from typing import Dict, List, Callable, Any


def group_by(aggregation_func: Callable, list_to_group: List):
    """
    Applies the aggregation_func to every list
    element to determine a grouping key
    """
    d = defaultdict(list)
    for item in list_to_group:
        d[aggregation_func(item)].append(item)

    return d


def get_value_from_dictionary(d: Dict, key: str, default=None, sep='.'):
    """Assume your dictionary has the following structure:
    {
        a: {a1: {a11: "a11"}, a2: "a"},
        b: "b1"
    }
    then this will happen
    > get_value_from_dictionary(d, "a.a1.a11", default=None, sep='.')
    > "a11"
    > get_value_from_dictionary(d, "a.a2", default=None, sep='.')
    > "a2"
    ...
    """
    path = key.split(sep)
    value = d
    for entry in path:
        if not isinstance(value, dict):
            return default
        value = value.get(entry, default)

    return value


def merge_dicts(dict1: Dict, dict2: Dict):
    """
    Merges two dictionaries together.
    Duplicate keys of dict2 override keys of dict1.
    """
    res = dict1.copy()
    res.update(dict2)
    return res


def safe_list_get(lst: List[Any], idx: int = 0, default=None) -> Any:
    """
    Helper method to extract one element safely out of a list.
    When index is out of bounds no exception is thrown,
    instead the specified default value is returned.
    :param lst: The list
    :param idx: Index of the element
    :param default: If the element does not exists return a default value
    :return: Element of the provided list
    """
    try:
        return lst[idx]
    except IndexError:
        return default


def chunk_list(lst: List[Any], chunk_size: int) -> List[Any]:
    """Partitions the given list into <chunk_size> chunks."""
    if len(lst) == 0:
        return lst

    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def make_list(item_or_items: Any) -> List[Any]:
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
    if hasattr(item_or_items, '__iter__') \
            and not isinstance(item_or_items, str):
        return list(item_or_items)

    return [item_or_items]
