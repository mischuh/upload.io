from abc import abstractmethod


class DataExporter:
    def __init__(self):
        self.errors = list()

    def format_errors(self, show_entries=5):
        if self.errors is None or len(self.errors) == 0:
            return None

        res = ""
        iterate_to = min(show_entries, len(self.errors))
        for i in range(0, iterate_to):
            res = res + str(self.errors[i]) + "\n"

        if len(self.errors) > show_entries:
            res += "There are {0} more errors".format(
                len(self.errors) - show_entries)

        return res

    @abstractmethod
    def export(self, options=None):
        """
        Triggers the hard work. Actual work depends on implementation.
        :return:
        """
        raise ValueError("You have to override this method.")

    @staticmethod
    def parser():
        """
        Returns the parser the exporter uses to scan the configuration.
        :return:
        """
        raise NotImplementedError()


class SimpleDatabaseExporter(DataExporter):

    def __init__(self):
        super().__init__()

    def export(self, options=None):
        pass

    @staticmethod
    def parser():
        """
        Returns the parser the exporter uses to scan the configuration.
        :return:
        """
        raise NotImplementedError()
