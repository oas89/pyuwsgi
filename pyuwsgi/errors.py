EXIT_CODE_STOP = 10
EXIT_CODE_REEXEC = 20
EXIT_CODE_APPLICATION_ERROR = 30
EXIT_CODE_UNHANDLED_EXCEPTION = 40


class ApplicationError(Exception):
    pass


class ConfigurationError(Exception):
    pass
