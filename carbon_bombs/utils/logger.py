import logging
import os
import platform
from os import path

WRITE_IN_FILE = False
FILE_OUT = None

# Get home path
HOME = os.path.expanduser("~")
DEFAULT_LOGGING_PATH = HOME

# default verbose level
DEFAULT_VERBOSE = 30


def get_logging_path():
    """Returns the path where logs are stored.
    You can choose a custom log file using this line:

    `logging.FILE_OUT = '/my/path/to/log/file.log'`

    The default path is the HOME except for Windows users for who it's
    the APPDATA path.

    Returns
    -------
    str
        Path to logs
    """
    if FILE_OUT is not None:
        # TODO: directory exists ? Nope you suck
        # TODO: is file
        return FILE_OUT

    # Get operating system
    ope_system = platform.system()

    log_path = DEFAULT_LOGGING_PATH

    if ope_system == "Windows":
        log_path = os.getenv("APPDATA")

    file_out = path.join(log_path, ".carbon_bombs.log")

    return file_out


def check_logger(logger):
    """Check logger object type.
    If a logger object is of type logging.Logger the object is return as is.
    Otherwise an Error is raised.

    Parameters
    ----------
    logger : logging.Logger
        logger object to display and dump application logs

    Returns
    -------
    logger : logging.Logger
        logger object to display and dump application logs

    Raises
    ------
    TypeError
        logger must be logging.Logger
    """
    if not isinstance(logger, logging.Logger):
        raise TypeError("logger must be logging.Logger")
    return logger


def get_logger(
    verbose=DEFAULT_VERBOSE, name=None, file_out=None, log=False, stdout=True
):
    """Create or get a logger object to output logs of an application.
    verbose level is set as following :

    +---------+----------+
    | verbose | level    |
    +=========+==========+
    | 50      | CRITICAL |
    | 40      | ERROR    |
    | 30      | WARNING  |
    | 20      | INFO     |
    | 10      | DEBUG    |
    | 0       | NOTSET   |
    +---------+----------+

    When using ``NOTSET`` or ``verbose=0`` it's the same as using ``WARNING`` or ``verbose=30``.

    - If name is None then defaulting to "logger".
    - If file_out is None
        - if log is False then no output is saved
        - if log is True then output is saved to default location
    - If log is False
        - if file_out is None then no output is saved
        - if file_out is a string the output is saved to file_out location
    - if stdout is False then no output is streamed to stdout

    Parameters
    ----------
    verbose : int or logger, default 30
        verbosity level from 0 to 50:

        0 is ``NOTSET``, 10 is ``DEBUG``, 20 is ``INFO``,
        30 is ``WARNING``, 40 is ``ERROR`` and 50 is ``CRITICAL``

    name : str, default None
        name to use for the logger
    file_out : str, default None
        file where to output logs,
        if a string is given `log` is set to True
    log : bool, default False
        whether to output logs to file or not
    stdout : bool, default False
        whether to output logs to stdout

    Returns
    -------
    logging.Logger
        logger object to display and dump application logs

    Raises
    ------
    ValueError
        name variable must be None or string
    TypeError
        verbose must be an integer (0, 10, 20, 30, 40, 50)
    ValueError
        file_out variable must be None or string
    ValueError
        Either one or both `log` and `stdout` parameters must be True

    Examples
    --------

    Basic usage as DEBUG

    .. code-block:: python

        >>> from carbon_bombs.utils.logger import get_logger

        >>> logger = get_logger(verbose=10)
        >>> print(logger)
        >>> logger.debug("DEBUG")
        >>> logger.info("INFO")
        >>> logger.warning("WARNING")
        >>> logger.error("ERROR")
        2022-08-23 16:09:55,622 - logger - DEBUG - DEBUG
        2022-08-23 16:09:55,623 - logger - INFO - INFO
        2022-08-23 16:09:55,624 - logger - WARNING - WARNING
        2022-08-23 16:09:55,625 - logger - ERROR - ERROR

    Changing the name and with INFO level

    .. code-block:: python

        >>> logger = get_logger(verbose=20, name="custom1")
        >>> logger.debug("DEBUG")
        >>> logger.info("INFO")
        >>> logger.warning("WARNING")
        >>> logger.error("ERROR")
        2022-08-23 16:09:55,626 - custom1 - INFO - INFO
        2022-08-23 16:09:55,627 - custom1 - WARNING - WARNING
        2022-08-23 16:09:55,629 - custom1 - ERROR - ERROR

    Changing the name, adding a file to store the logs and with WARNING level

    .. code-block:: python

        >>> logger = get_logger(verbose=30, name="custom2", file_out="file.log")
        >>> logger.debug("DEBUG")
        >>> logger.info("INFO")
        >>> logger.warning("WARNING")
        >>> logger.error("ERROR")
        2022-08-23 16:09:55,626 - custom1 - INFO - INFO
        2022-08-23 16:09:55,627 - custom1 - WARNING - WARNING


    Changing the name, adding a file to store the logs and with ERROR level

    .. code-block:: python

        >>> logger = get_logger(verbose=40, name="custom3", file_out="file.log", log=True)
        >>> logger.debug("DEBUG")
        >>> logger.info("INFO")
        >>> logger.warning("WARNING")
        >>> logger.error("ERROR")
        2022-08-23 16:09:55,629 - custom1 - ERROR - ERROR
    """

    LOGGING_LEVEL = {
        0: logging.NOTSET,
        10: logging.DEBUG,
        20: logging.INFO,
        30: logging.WARNING,
        40: logging.ERROR,
        50: logging.CRITICAL,
    }

    if name is None:
        name = "logger"
    if not isinstance(name, str):
        raise ValueError("name variable must be None or string")
    if verbose is None:
        verbose = 0
    elif not isinstance(verbose, int):
        raise TypeError("verbose must be an integer (0, 10, 20, 30, 40, 50)")
    elif verbose not in (0, 10, 20, 30, 40, 50):
        raise ValueError("verbose must be an integer (0, 10, 20, 30, 40, 50)")

    # FIXME Beware if defaults are manually changed
    # it might override behavior by forcing output
    # while explicitely set to None or False
    log = WRITE_IN_FILE if not log else log
    log = True if file_out else log
    file_out = FILE_OUT if file_out is None else file_out

    if file_out is None:
        file_out = get_logging_path()
    if not isinstance(file_out, str):
        raise ValueError("file_out variable must be None or string")

    if log is False and stdout is False:
        raise ValueError(
            "Either one or both `log` and `stdout` parameters must be True"
        )

    level = min(verbose // 10 * 10, 50)

    # create formatter
    formatter = logging.Formatter(
        "%(asctime)s - %(module)s - %(funcName)s - %(message)s"
    )

    # create logger with name variable
    logger = logging.getLogger(name)
    logger.propagate = False

    # Reset handlers in case this name has already be used
    logger.handlers = []

    logger.setLevel(LOGGING_LEVEL[level])
    handlers = [str(x) for x in logger.handlers]
    # create file handler which logs even debug messages
    if log:
        fh = logging.FileHandler(file_out, encoding="utf-8-sig")
        fh.setLevel(LOGGING_LEVEL[level])
        # add the formatter to the handler
        fh.setFormatter(formatter)
        # add the handler to the logger
        if str(fh) not in handlers:
            logger.addHandler(fh)

    # create console handler with a custom verbose level
    if stdout:
        ch = logging.StreamHandler()
        ch.setLevel(LOGGING_LEVEL[level])
        # add the formatter to the handler
        ch.setFormatter(formatter)
        # add the handler to the logger
        if str(ch) not in handlers:
            logger.addHandler(ch)

    return logger


# create custom logger for the project
LOGGER = get_logger(name="carbon_bombs", log=True)
