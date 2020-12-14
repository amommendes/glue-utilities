from abc import ABC, abstractmethod

class Exporter(ABC):
    """
        Abstract Exporter to be implemented by exporters with distinct modes (e.g., s3, jbdc)
    """
    @abstractmethod
    def read_databases(self):
        pass

    @abstractmethod
    def export_datacatalog(self):
        pass

    @abstractmethod
    def run(self):
        pass