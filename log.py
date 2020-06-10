import logging


LEVEL = 'DEBUG'
FORMAT = '%(asctime)s | Module: %(module)s | Function: %(funcName)s | Message: %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'


def setup_custom_logger(name):
    """
    Sets up a customer logger for use in the application. The logging
    level is set to DEBUG by default, meaning all debug messages will
    be logged to the terminal as the application executes.

    Set LEVEL to 'INFO' to disable the debug information.

    Keyword Arguments:
    name -- __name__ property of module you are instantiating a logger
    object from.
    """
    formatter = logging.Formatter(fmt=FORMAT, datefmt=DATEFMT)

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(LEVEL)
    logger.addHandler(handler)

    return logger
