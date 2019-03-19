import logging
import string
import sys
from logging.handlers import TimedRotatingFileHandler

# libraries to install
# pip install buffered_smtp_handler

# information about the email sender and server
from KYLIN_USB.sources.cube_system.global_variables import LoggerProperties

HOST = "Host Server"
PORT = "Port of SMTP Server"
SENDER = 'senger@gmail.com'
TO = ['receiver@gmail.com']
SUBJECT = 'This is not a subject'

USERNAME = "username@gmail.com"
PASSWORD = "password"



# handlers for our logger

def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(LoggerProperties.FORMATTER))
    return console_handler


def get_file_handler():
    # create a new log for each day
    file_handler = TimedRotatingFileHandler(LoggerProperties.LOG_FILE, when='midnight')
    file_handler.setFormatter(logging.Formatter(LoggerProperties.FORMATTER))
    return file_handler


class SMTP_handler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, capacity):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        self.mailhost = mailhost
        self.mailport = None
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.setFormatter(logging.Formatter(LoggerProperties.FORMATTER))

    def flush(self):
        if len(self.buffer) > 0:
            try:
                import smtplib
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                smtp = smtplib.SMTP(self.mailhost, port)
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (
                    self.fromaddr, string.join(self.toaddrs, ","), self.subject)
                for record in self.buffer:
                    s = self.format(record)
                    print
                    s
                    msg = msg + s + "\r\n"
                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except:
                self.handleError(None)  # no particular record
            self.buffer = []


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
    logger.addHandler(get_console_handler())
    logger.addHandler(get_file_handler())
    # with this pattern, it's rarely necessary to propagate the error up to parent
    logger.propagate = False
    return logger


def test_logging_email():
    logger = get_logger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(SMTP_handler(mailhost=(HOST, PORT), fromaddr=SENDER, toaddrs=TO, subject=SUBJECT,
                                   credentials=(USERNAME, PASSWORD)))

    # debugging
    for i in range(100):
        logger.info("spark_submit = %d", i)


if __name__ == "__main__":
    test_logging_email()
