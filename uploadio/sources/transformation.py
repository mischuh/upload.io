import abc
from enum import Enum
from typing import Any, Dict, Optional, Type, Union


class Task:
    """
    DSL to describe a :py:class:`Task`, the key of a :py:class:`Transformation`
    """

    def __init__(self, name: str, operator: Union[str, Dict]) -> None:
        super().__init__()
        self.name = name
        self.operator = operator

    def operate(self, *args, **kwargs) -> Any:
        self._operate(*args, **kwargs)

    @abc.abstractmethod
    def _operate(self, *args, **kwargs) -> Any:
        raise NotImplementedError()

    def __repr__(self) -> str:
        return "Task(name='{}', operator='{}')" \
            .format(self.name, self.operator)


class TransformationType(Enum):
    RULE = 'rule'
    FILTER = 'filter'


class Transformation:
    """
    Holds the transformations

    Transformations could be either functions to apply,
    filter or anything you can imagine
    """

    def __init__(
            self,
            type: TransformationType,
            task: Task,
            order: Optional[int]) -> None:
        self.type = type
        self.task = task
        self.order = order

    def transform(self, *args, **kwargs) -> Any:
        return self._transform(*args, **kwargs)

    @abc.abstractmethod
    def _transform(self, *args, **kwargs) -> Any:
        raise NotImplementedError("You have to implement this method")

    def __repr__(self) -> str:
        return "{}(type={}, order={}, task={})" \
            .format(self.__class__.__name__, self.type, self.order, self.task)


class ReplaceRuleTransformation(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        # validate.is_in_dict_keys('old', task.operator)
        # validate.is_in_dict_keys('new', task.operator)
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str = '', *args, **kwargs) -> str:
        old = self.task.operator['old']
        new = self.task.operator['new']
        return value.replace(old, new)


class RegexReplaceTransformation(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value, *args, **kwargs) -> Any:
        import re
        old = self.task.operator['old']
        new = self.task.operator['new']
        return re.sub(r'{}'.format(old), new, value)


class UppercaseRuleTransformation(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str = '', *args, **kwargs) -> str:
        return value.upper()


class LambdaRuleTransformation(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value, *args, **kwargs) -> Any:
        assert self.__validate() is True
        # TODO: Is there a safer way to eval any function?!
        func = eval(self.task.operator)
        return func(value)

    def __validate(self) -> bool:
        import types
        return isinstance(eval(self.task.operator), types.LambdaType) \
            and eval(self.task.operator).__name__ == "<lambda>"


class DateFormatTransformation(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str = '', *args, **kwargs) -> str:
        import datetime
        from_fmt = self.task.operator['from']
        to_fmt = self.task.operator['to']
        return datetime.datetime.strptime(value, from_fmt).strftime(to_fmt)


class NumericComparisonFilter(Transformation):

    def __init__(self, task: Task, order: Optional[int]) -> None:
        import numbers
        # validate.is_in_dict_keys('expression', task.operator)
        # validate.is_in_dict_keys('other', task.operator)
        # validate.is_instance_of(task.operator['other'], numbers.Number)
        # validate.is_in_list(
        #     task.operator['expression'],
        #     ['lt', 'le', 'eq', 'ne', 'ge', 'gt']
        # )
        super().__init__(TransformationType.FILTER, task, order)

    def _transform(self, value: Union[int, float], *args, **kwargs) -> bool:
        """
        Perform “rich comparisons” between a and b.
        Specifically
            * lt(a, b) is equivalent to a < b
            * le(a, b) is equivalent to a <= b
            * eq(a, b) is equivalent to a == b
            * ne(a, b) is equivalent to a != b
            * gt(a, b) is equivalent to a > b
            * ge(a, b) is equivalent to a >= b
        :returns:
            if condition not matches (e.g value (a=50) < other (b=10))
            we want to tell the executing function that we want to
            filter out this value/row
        """
        import operator
        try:
            method = getattr(operator, self.task.operator['expression'])
            return not method(value, self.task.operator['other'])
        except AttributeError:
            raise NotImplementedError(
                f"Class `{operator.__class__.__name__}` \
                does not implement `{self.task.operator['expression']}`"
            )


class TransformationFactory:

    # TODO: Make it more flexible and reproducible
    __MAPPING: Dict[str, Type[Transformation]] = {
        "replace": ReplaceRuleTransformation,
        "regexreplace": RegexReplaceTransformation,
        "uppercase": UppercaseRuleTransformation,
        "lambda": LambdaRuleTransformation,
        "date_format": DateFormatTransformation,
        "comparison": NumericComparisonFilter
    }

    @staticmethod
    def load(name: str) -> Type[Transformation]:
        # validate.is_in_dict_keys(name, TransformationFactory.__MAPPING)
        return TransformationFactory.__MAPPING[name]
