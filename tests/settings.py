from os import path

try:
    import ConfigParser as configparser
except ImportError:
    import configparser # Python 3


__all__ = [
    'host',
    'port',
    'database',
    'user',
    'password',
    'min_conn',
    'max_conn',
    'cleanup_timeout'
]


config = configparser.ConfigParser()
config.read(path.join(path.dirname(__file__), 'database.cfg'))


host = config.get('default', 'host')
port = config.getint('default', 'port')
database = config.get('default', 'database')
user = config.get('default', 'user')
password = config.get('default', 'password')
min_conn = config.getint('default', 'min_conn')
max_conn = config.getint('default', 'max_conn')
cleanup_timeout = config.getint('default', 'cleanup_timeout')
