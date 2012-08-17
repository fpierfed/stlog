"""
stlog

A Python logging module layered uplon the Python Standard Library logging module
with the added ability to
    - Log all messages to a database (using the stdb module).
    - Automatically fallback to logging to STDERR when a database connection is
      not available.

Example Usage
    import stlog


    stlog.init('', '/tmp/test.db', db_type='sqlite')
    # If the database does not exist:
    # stlog._mkdb()
    logger = stlog.get_logger()

    ... some code ...
    logger.debug('some debug message')

    ... some code ...
    logger.warn('some warning')

    ... some code ...
    logger.info('some informational message')

    ... some code ...
    logger.error('some error message')

    ... some code ...
    logger.critical('some critical error message')
"""
import datetime
import logging
import os
import socket
import urllib
import elixir



DEFAULT_LOG_FMT = '%(levelname)s - ' + \
                  '%(hostname)s - ' + \
                  '%(asctime)s %(module)s.%(funcName)s (%(filename)s) - ' + \
                  '%(message)s'
LOG_FMT = os.environ.get('STLOG_FMT', DEFAULT_LOG_FMT)



class InvalidConfiguration(Exception):
    """
    Custon exception class.
    """
    pass


class Events(elixir.Entity):
    """
    Database class representing a Pyhon logging.LogRecord instance.
    See http://docs.python.org/library/logging.html#logrecord-attributes for a
    description of the attributes.
    """
    elixir.using_options(tablename='logentry')

    # elixir creates an integer primary key called id automatically.
    # TODO: Unicode(255) is way too big for these varchar fields.
    datetime = elixir.Field(elixir.DateTime, required=True)
    asctime = elixir.Field(elixir.Unicode(255))
    created = elixir.Field(elixir.Float)
    hostname = elixir.Field(elixir.Unicode(255), required=True)
    filename = elixir.Field(elixir.Unicode(255))
    funcName = elixir.Field(elixir.Unicode(255))
    levelname = elixir.Field(elixir.Unicode(255))
    levelno = elixir.Field(elixir.Unicode(255))
    lineno = elixir.Field(elixir.Integer)
    module = elixir.Field(elixir.Unicode(255))
    msecs = elixir.Field(elixir.Integer)
    message = elixir.Field(elixir.Unicode(255), required=True)
    name = elixir.Field(elixir.Unicode(255))
    pathname = elixir.Field(elixir.Unicode(255))
    process = elixir.Field(elixir.Integer)
    processName = elixir.Field(elixir.Unicode(255))
    relativeCreated = elixir.Field(elixir.Integer)
    thread = elixir.Field(elixir.Integer)
    threadName = elixir.Field(elixir.Unicode(255))
    exception = elixir.Field(elixir.Unicode(255))

    # Default log entry format.
    fmt = LOG_FMT

    def __repr__(self):
        return(self.fmt % self.__dict__)


class DatabaseHandler(logging.Handler):
    """
    Logging handler to write log events to the database.
    """
    def __init__(self):
        super(DatabaseHandler, self).__init__()

        self._fallback_handler = logging.StreamHandler()

        # Connect to the database. The connection is closed in self.close()
        elixir.setup_all()
        return

    def setFormatter(self, formatter):
        """
        Simply update self._fallback_handler
        """
        # Since we have no control over these names:
        # pylint: disable=C0103
        super(DatabaseHandler, self).setFormatter(formatter)
        self._fallback_handler.setFormatter(formatter)
        return

    def emit(self, record):
        """
        Insert `record` in the database.
        """
        # Since elixir.session is built on the fly, pylint complaints that it
        # has no commit method, hence:
        # pylint: disable=E1101
        # Insert the entry in the database and quit.
        # Here there are a few annoyances: the docs say that LogRecord instances
        # (i.e. record in our case) have a few attributes such as asctime. In
        # reality they do not always have them: some of them (again asctime in
        # particular) are created by Formatter instances when they operate on
        # them.
        self.format(record)
        entry = Events(datetime=record.datetime,
                       asctime=record.asctime,
                       created=record.created,
                       hostname=record.hostname,
                       filename=record.filename,
                       funcName=record.funcName,
                       levelname=record.levelname,
                       levelno=record.levelno,
                       lineno=record.lineno,
                       module=record.module,
                       msecs=record.msecs,
                       message=record.message,
                       name=record.name,
                       pathname=record.pathname,
                       process=record.process,
                       processName=record.processName,
                       relativeCreated=record.relativeCreated,
                       thread=record.thread,
                       threadName=record.threadName,
                       exception=record.exception)
        try:
            elixir.session.commit()
        except:
            elixir.session.rollback()
            return(self.handleError(record))
        return

    def handleError(self, record):
        """
        This is called in case of errors/exceptions being raised in self.emit().
        The standard implementation raises an exception if
        logging.raiseExceptions == True and does nothing otherwise. We fallback
        to a StreamHandler instead.
        """
        # Since we have no control over these names:
        # pylint: disable=C0103
        return(self._fallback_handler.emit(record))

    def close(self):
        """
        Close the session.
        """
        # Since elixir.session is built on the fly, pylint complaints that it
        # has no close method, hence:
        # pylint: disable=E1101
        # elixir.session.close()
        return(super(DatabaseHandler, self).close())

    def format(self, record):
        """
        We overide super.format() to "better" handle timestamps and exception
        messages, if present. Also, add the hostname attribute, otherwise not
        present.
        """
        # Add our custon fields first.
        if(not hasattr(record, 'hostname')):
            record.hostname = socket.gethostname()
        if(not hasattr(record, 'datetime')):
            record.datetime = datetime.datetime.fromtimestamp(record.created)
        if(record.exc_info):
            df_fmtr = logging._defaultFormatter
            record.exception = df_fmtr.formatException(record.exc_info)
        else:
            record.exception = None

        # Now call super.format() to make sure everything is kosher.
        super(DatabaseHandler, self).format(record)

        # Then massage the standsrd ones.
        if(len(record.asctime) >= 4 and record.asctime[-4] == ','):
            record.asctime = record.asctime[:-4] + '.' + record.asctime[-3:]

        # Turn every string into unicode.
        for attr in ('asctime',
                     'hostname',
                     'filename',
                     'funcName',
                     'levelname',
                     'levelno',
                     'module',
                     'message',
                     'name',
                     'pathname',
                     'processName',
                     'threadName',
                     'exception'):
            value = getattr(record, attr)
            if(value is not None and isinstance(value, str)):
                setattr(record, attr, unicode(value))
        return


def _db_connection_str(flavor, username, password, server, port, database):
    """
    Internal convenience function to derive the appropriate database coneection
    string.
    """
    if(flavor == 'sqlite'):
        return('sqlite:///%s' %(os.path.abspath(database)))

    has_mssql = flavor.startswith('mssql')
    port_info = ''
    connection_tmplt = '%(flavour)s://%(user)s:%(passwd)s@%(host)s'

    # We need to handle a few special cases.
    # 0. The password miught contain characters that need to be escaped.
    pwd = urllib.quote_plus(password)

    # 1. Database separator
    db_info = '/' + database

    # 2. Yes/No port onformation and yes/no MSSQL.
    if(port and port != -1 and not has_mssql):
        port_info += ':' + str(port)
    elif(port and port != -1):
        port_info += '?port=' + str(port)

    # 3. MSSSQL wants a different connection string if a port is specified. Bug?
    if(has_mssql):
        connection_tmplt += '%(db_info)s%(port_info)s'
    else:
        connection_tmplt += '%(port_info)s%(db_info)s'

    connection_str = connection_tmplt % {'flavour': flavor,
                                         'user': username,
                                         'passwd': pwd,
                                         'host': server,
                                         'port_info': port_info,
                                         'db_info': db_info}
    return(connection_str)

def init(server, database, username=None, password=None, db_type='mssql'):
    """
    Intialize the connection to a given database using functionality from stpydb


    Usage
    The basic usage is to specify the name of the database server and of the
    database to connect to:
        init(server, database)
    In this mode, it will be assumed that `username` == $USER (i.e. the current
    user account the script is executed as) and that `password` is to be read
    from $ACAREA/`username`.dat (if $ACAREA is not defined in the user
    environment, it defaults to '/usr/local/sybase/stbin'). The `username`.dat
    file is in the form
        server password

    Username can be spedified instead of relying on $USER. In this case, the
    behavior is the same, only setting `username` to whatever value the caller
    passed.

    In cases where it might be desirable to bypass the $ACAREA/`username`.dat
    authentication aid completely, `password` can be specified, in addition to
    `username`.

    IF you know what to do, you can mess with `db_type` and support different
    database vendors, not just the default MS SQL Server.

    Raise
        InvalidConfiguration exception if `username` is not specified and cannot
        be derived from the environment (i.e. no $USER and no $LOGNAME).

        InvalidConfiguration exception is `password` is not specified and cannot
        be derived from $ACAREA/`username`.dat given `server`.
    """
    acarea = os.environ.get('ACAREA', '/usr/local/sybase/stbin')

    if(username is None and db_type != 'sqlite'):
        msg = 'Unable to derive username from either $USER or $LOGNAME.'
        try:
            username = os.environ['USER']
        except KeyError:
            try:
                username = os.environ['LOGNAME']
            except:
                raise(InvalidConfiguration(msg))

    if(password is None and db_type != 'sqlite'):
        path = os.path.join(acarea, '%s.dat' % (username))
        try:
            passwords = dict([l.split() for l in open(path)])
        except:
            msg = 'Unable to access %s to retrieve the database password.'
            raise(InvalidConfiguration(msg % (path)))
        try:
            password = passwords[server]
        except:
            msg = 'Unable to find password for %s in %s.'
            raise(InvalidConfiguration(msg % (server, path)))

    elixir.metadata.bind = _db_connection_str(db_type,
                                              username,
                                              password,
                                              server,
                                              None,
                                              database)
    elixir.metadata.bind.echo = False
    elixir.setup_all()
    if(db_type == 'sqlite' and not os.path.exists(database)):
        elixir.create_all()
    return

def get_logger(level=logging.DEBUG):
    """
    Return a logger object that can be used to emit log messages to the database
    defined when calling stlog.init(). In case of problems communicating with
    the database, log messages are printed to STDERR instead.
    """
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    db_handler = DatabaseHandler()
    db_handler.setFormatter(logging.Formatter(Events.fmt))
    logger.addHandler(db_handler)
    return(logger)



