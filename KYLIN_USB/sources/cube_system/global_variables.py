

class KylinProperties(object):
    HEADERS = {'Content-type': 'application/json'}
    HOSTNAME = "127.0.0.1"
    AUTH = ("admin", "KYLIN")
    KYLIN_DIRECTORY = "/opt/kylin/"


SIZE_POOL = 5


class LoggerProperties(object):
    # configuration of the loggers
    FORMATTER = "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
    # where the log files of our script are stored
    LOG_FILE = "kylin_script_log"
