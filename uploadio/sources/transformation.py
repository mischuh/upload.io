import abc
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from src.p3common.common import validators as validate
from uploadio.utils import auto_str


class Task:
    """
    DSL to describe a :py:class:`Task`, the key of a :py:class:`Transformation`
    """

    def __init__(self, *args) -> None:
        self.operator = None

    def operate(self, *args, **kwargs) -> Any:
        self._operate(*args, **kwargs)

    @abc.abstractmethod
    def _operate(self, *args, **kwargs) -> Any:
        raise NotImplementedError()


class RuleTask(Task):
    """
    Describes the :py:class:`Task` for a :py:class:`RuleTransformation`
    """

    def __init__(self, name: str, operator: Union[str, Dict]) -> None:
        super().__init__()
        self.name = name
        self.operator = operator

    def __repr__(self) -> str:
        return "RuleTask(name='{}', operator='{}')" \
            .format(self.name, self.operator)


class FilterTask(Task):
    """
    Describes the :py:class:`Task` for a :py:class:`FilterTransformation`

    A :py:class:`FilterTask` can only be applied on catalog level
    not on a :py:class:`Field`
    """

    def __init__(self, attribute: str, operator: str, expression: str) -> None:
        super().__init__()
        self.attribute = attribute
        self.operator = operator
        self.expression = expression

    def __repr__(self) -> str:
        return "FilterTask(attribute='{}', operator='{}', " \
            "expression='{}')".format(self.attribute,
                                      self.operator,
                                      self.expression)


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

    def __init__(self, task: RuleTask, order: Optional[int]) -> None:
        validate.is_in_dict_keys('old', task.operator)
        validate.is_in_dict_keys('new', task.operator)
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str ='', *args, **kwargs) -> str:
        old = self.task.operator['old']
        new = self.task.operator['new']
        return value.replace(old, new)


class UppercaseRuleTransformation(Transformation):

    def __init__(self, task: RuleTask, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str='', *args, **kwargs) -> str:
        validate.is_str(value)
        return value.upper()


class LambdaRuleTransformation(Transformation):

    def __init__(self, task: RuleTask, order: Optional[int]) -> None:
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

    def __init__(self, task: RuleTask, order: Optional[int]) -> None:
        super().__init__(TransformationType.RULE, task, order)

    def _transform(self, value: str='', *args, **kwargs) -> str:
        validate.is_str(value)
        import datetime
        from_fmt = self.task.operator['from']
        to_fmt = self.task.operator['to']
        return datetime.datetime.strptime(value, from_fmt).strftime(to_fmt)


class FilterTransformation(Transformation):
    """
    A :py:class:`FilterTranformation` can only be applied on a catalog itself.

    For example, you can filter out those lines of a file that don't
    fit a specifit criteria.
    """

    def __init__(
            self,
            task: FilterTask,
            order: Optional[int],
            *args,
            **kwargs) -> None:
        super().__init__(TransformationType.FILTER, task, order)
        self.args = args
        self.kwargs = kwargs

    def _transform(self, row, *args, **kwargs) -> bool:
        """
        Wether a specific criteria fits for a givenrow
        :param row:
        :param args:
        :param kwargs:
        :return:
        """
        print(row)
        return True


class TransformationFactory:

    # TODO: Make it more flexible and reproducible
    __MAPPING: Dict[str, Type[Transformation]] = {
        "replace": ReplaceRuleTransformation,
        "uppercase": UppercaseRuleTransformation,
        "lambda": LambdaRuleTransformation,
        "date_format": DateFormatTransformation
    }

    @staticmethod
    def load(name: str) -> Type[Transformation]:
        validate.is_in_dict_keys(name, TransformationFactory.__MAPPING)
        return TransformationFactory.__MAPPING[name]
