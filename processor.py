"""
Hold the aggregator class and its template classes
"""
import logging
from copy import deepcopy
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class DataIterator(ABC):
    """ Implements the data source implementing an iterator that
    returns a dict{key_str: value, ..} using ColumnMapper
    """

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __next__(self) -> dict:
        pass


class Operator(ABC):
    """ Operator class template
    """

    def run(self, data):
        """ Abstract method to update the match with the data row """
        pass

    def output(self):
        """ Invokes internal operator readouts or returns real data
        """
        pass


class Matcher(Operator):
    """ Matcher performs and AND match on its list, then invokes operators
    """

    def __init__(self, name: str, matches: list):
        self.matches = matches
        self.operators = []
        self.name = name

    def add_operator(self, operator: Operator):
        self.operators.append(operator)

    def run(self, data):
        match = True
        if self.matches:
            for label, value in self.matches:
                if data[label] != value:
                    match = False
                    break
        if not match:
            return

        logger.debug("Matcher '%s' '%s' hit.", self.name, self.matches)
        for operator in self.operators:
            operator.run(data)

    def output(self):
        output = {}
        for name, operator in self.operators:
            output[name] = operator.output()
        return (self.name, output)


class Filter(Operator):
    """ Filter invokes operators if no matches occur inside its list
    """

    def __init__(self, name: str, matches: list):
        self.matches = matches
        self.operators = []
        self.name = name

    def add_operator(self, operator: Operator):
        self.operators.append(operator)

    def run(self, data):
        match = True
        if self.matches:
            for label, value in self.matches:
                if data[label] != value:
                    match = False
                    break
        if match:
            logger.debug("Filter '%s' '%s' hit.", self.name, self.matches)
            return

        for operator in self.operators:
            operator.run(data)

    def output(self):
        output = {}
        for name, operator in self.operators:
            output[name] = operator.output()
        return (self.name, output)


class Grouper(Operator):
    """ Matcher performs and AND match on its list, then invokes operators
    """

    def __init__(self, name: str, group_by: str):
        self.group_by = group_by
        self.operators = []
        self.name = name
        self.groups = {}

    def add_operator(self, operator: Operator):
        self.operators.append(operator)

    def run(self, data):
        if data[self.group_by] not in self.groups:
            self.groups[data[self.group_by]] = deepcopy(self.operators)
            logger.debug(
                "Grouper '%s' '%s' created new group '%s'",
                self.name,
                self.group_by,
                data[self.group_by],
            )
        for operator in self.groups[data[self.group_by]]:
            operator.run(data)

    def output(self):
        output = {}
        for name, group in self.groups.items():
            output[name] = {}
            for item_name, item in group:
                output[name][item_name] = item
        return (self.name, output)


class Summer(Operator):
    """
    Sum a variable
    """

    def __init__(self, name: str, var_name: str):
        self.var_name = var_name
        self.name = name
        self.total = 0

    def run(self, data):
        try:
            self.total += data[self.var_name]
            logger.debug(
                "Summer '%s' '%s' total %s", self.name, self.var_name, self.total
            )
        except TypeError:
            logger.info(
                "Summer '%s' '%s' total %s - cowardly not adding non-numeric type.",
                self.name,
                self.var_name,
                self.total,
            )

    def output(self):
        return (self.name, self.total)


class Processor:
    """ Machinery to apply the matches to the data from the datasource."""

    def __init__(self, data_iterator: DataIterator, name: str = None):
        """
        Iterate over the data_iterator grouping matches by match.
        """
        self.data_iterator = data_iterator

        self.operators = list()

    def add_operator(self, new_operator: Operator):
        """
        Append a match to the list of things to aggregate
        """
        self.operators.append(new_operator)

    def start(self):
        """
        Iterate over the data_iterator, matching and grouping.
        """
        for data in self.data_iterator:
            for operator in self.operators:
                operator.run(data)

    def output(self):
        output = {}
        for operator in self.operators:
            name, result = operator.output()
            output[name] = result
        return output
