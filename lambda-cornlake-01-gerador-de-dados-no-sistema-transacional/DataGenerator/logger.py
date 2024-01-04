import logging
import os 

def info(content: object):
    logging.info(content)
    
def error(content: object):
    logging.error(content)
    
def warning(content: object):
    logging.warning(content)
    
def debug(content: object):
    if os.environ.get('AWS_SAM_LOCAL') == 'true':
        logging.debug(content)
    
def critical(content: object):
    logging.critical(content)

def configure_logger():
    logging.basicConfig(
        #filename='log.log', 
        level=logging.INFO, 

        format='%(asctime)s %(levelname)s %(message)s'
    )
    logging.info('Logger configured')
    return logging