STOPPING = 10
RELOADING = 20
APPLICATION_ERROR = 30
UNHANDLED_EXCEPTION = 40


class ApplicationError(Exception):
    pass


class ConfigurationError(Exception):
    pass
