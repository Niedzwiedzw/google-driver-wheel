from flask import Flask, jsonify
from csv import reader
from json import dumps

import typing as t
import os
from datetime import datetime, timedelta
from subprocess import check_output, CalledProcessError, run
import hashlib
from sys import argv

DEBUG = bool(os.environ.get('DEBUG'))
DIRECTORY = '/tmp'
CACHE_TIME: timedelta = timedelta(seconds=5)
ANCIENT_TIME = datetime(1993, 2, 13, 0, 0, 0, 0)
APP_DIR = '/app' if not DEBUG else '.'


def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs)


def absolute(filename_: str) -> str:
    return os.path.join(DIRECTORY, filename_)


def checksum(url: str) -> str:
    return hashlib.sha256(url.encode('utf-8')).hexdigest()


def file_data(url: str) -> (str, str):
    return str(datetime.now().timestamp()), checksum(url)


def retrieve_data(filename_: str) -> t.Optional[t.Tuple[str, str]]:
    split = filename_.split('_')
    return tuple(split) if len(split) == 2 else None


def filename(url: str) -> (str, str):
    return '_'.join(file_data(url))


def matches_hash(path: str, hash_: str) -> bool:
    data = retrieve_data(path)
    return data[1] == hash_ if data else False


def already_saved(hash_: str) -> t.Generator[str, None, None]:
    yield from (f for f in os.listdir(DIRECTORY) if matches_hash(absolute(f), hash_))


def valid_cache(url: str) -> t.Optional[str]:
    time, hash_ = file_data(url)
    try:
        latest = next((f for f in sorted(
            already_saved(hash_),
            key=lambda f: retrieve_data(absolute(f[1])) or ANCIENT_TIME, reverse=True,
        )))
    except StopIteration:
        return None

    delta = datetime.fromtimestamp(float(time)) - datetime.fromtimestamp(float(retrieve_data(latest)[0]))

    return latest if delta <= CACHE_TIME else None


def get_file_handle(url: str) -> str:
    cache = valid_cache(url)
    return absolute(cache) if cache else create_new_handle(url)


def create_new_handle(url: str) -> t.Optional[str]:
    dprint(f'DEBUG creating new file for {url}')
    filename_ = filename(url)
    path = absolute(filename_)

    command = [
            os.path.join(APP_DIR, 'goodls_linux_amd64'),
            '-u',
            url,
            '-e',
            'csv',
            '--overwrite',
            '--directory',
            DIRECTORY,
            '--filename',
            filename_,
        ]

    command = ' '.join(command)

    dprint('[DEBUG]', command)
    try:
        dprint('DEBUG: ', check_output(command, shell=True))
    except CalledProcessError:
        return None

    return path


def get_gdrive_contents(url: str) -> t.Optional[t.List[t.List[str]]]:
    path = get_file_handle(url)
    if not path:
        return [[]]
    with open(path, 'r') as f:
        r = reader(f)
        return list(map(list, r))


app = Flask(__name__)


@app.route('/document/<path:url>')
def file(url: str):
    url = url.strip()
    if url.endswith('edit'):
        url = url + '?usp\\=sharing'

    dprint('[DEBUG]', url)
    return jsonify(get_gdrive_contents(url or ''))


if __name__ == '__main__':
    url_ = argv[2]
    print(get_gdrive_contents(url_))
