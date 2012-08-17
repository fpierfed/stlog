#!/usr/bin/python
import stlog



DB = '/tmp/test.db'
stlog.init('', DB, db_type='sqlite')
logger = stlog.get_logger();

# Module-level logging
logger.debug('debug message')
logger.warn('warning')
logger.info('informational message')
logger.error('error message')
logger.critical('critical error')

# Function-level
def foo(logger):
    logger.debug('debug message')
    logger.warn('warning')
    logger.info('informational message')
    logger.error('error message')
    logger.critical('critical error')
    return


if(__name__ == '__main__'):
    # Module-level again
    logger.debug('debug message')
    logger.warn('warning')
    logger.info('informational message')
    logger.error('error message')
    logger.critical('critical error')

    foo(logger)
