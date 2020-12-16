import json
from glue_migrator.utils.logger import Logger

logger = Logger()
logger.basicConfig()


class ConnectionHandler:
    """
    Handles connection the connection file or connection name and returns a connection object
    """

    def __init__(self, glue_context=None, connection_name=None, file_path=None):
        self.glue_context = glue_context
        self.connection_name = connection_name
        self.file_path = file_path

    @staticmethod
    def _get_credentials_file(path):
        """
        Get a json file in the self.file_path and returns a dict with credentials data
        :param path: path to file with credentials data
        :return: dict with credentials data
        Example credentials file content:
        >>> {
        ...     url:  "jdbc:protocol://hostname",
        ...     user: "user",
        ...     password: "pass"
        ... }
        """

        logger.info(f"Reading credentials file in path {path}")
        try:
            with open(path, "r") as credentials:
                credentials = json.load(credentials)
            logger.info(f"Getting file credentials succeed.")
            return credentials
        except Exception as error:
            raise RuntimeError(f"Error while reading credentials file: {error}")

    @staticmethod
    def _get_glue_credentials(glue_context, connection_name):
        """
        Make request to glue api through glue context for getting credentials data
        :param glue_context: GlueContext object
        :param connection_name: string with connection name available in the Glue connections
        :return:
        """
        logger.info(f"Reading credentials from Glue JDBC connection")
        glue_context.extract_jdbc_conf(connection_name)

    def get_credentials(self):
        """
        Get credentials depending on source (glue context or file)
        :return: dict with credentials
        """
        if self.file_path is not None:
            return self._get_credentials_file(self.file_path)
        elif (self.glue_context is not None) and (self.connection_name is not None):
            return self.glue_context.extract_jdbc_conf(self.connection_name)
        else:
            raise RuntimeError(
                "You need provide glue_context with connection_name or the file"
                " path for credentials file"
            )
