from flask import Flask

import typing as t
import os
from datetime import datetime, timedelta
from subprocess import check_output, CalledProcessError
import hashlib

DIRECTORY = '/tmp'
CACHE_TIME: timedelta = timedelta(seconds=5)
ANCIENT_TIME = datetime(1993, 2, 13, 0, 0, 0, 0)


def absolute(filename_: str) -> str:
    return os.path.join(DIRECTORY, filename_)


def checksum(url: str) -> str:
    return hashlib.sha256(url.encode('utf-8')).hexdigest()


def file_data(url: str) -> (str, str):
    return str(datetime.now()), checksum(url)


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
        latest = next((f for f in sorted(already_saved(hash_), key=lambda f: retrieve_data(absolute(f[1])) or ANCIENT_TIME, reverse=True)))
    except StopIteration:
        return None

    delta = datetime.fromisoformat(time) - datetime.fromisoformat(retrieve_data(latest)[0])

    return latest if delta <= CACHE_TIME else None


def get_file_handle(url: str) -> str:
    cache = valid_cache(url)
    return absolute(cache) if cache else create_new_handle(url)


def create_new_handle(url: str) -> t.Optional[str]:
    print(f'DEBUG creating new file for {url}')
    filename_ = filename(url)
    path = absolute(filename_)

    try:
        check_output([
            './goodls_linux_amd64',
            '-u',
            url,
            '-e',
            'csv',
            '--overwrite',
            '--directory',
            DIRECTORY,
            '--filename',
            filename_,
        ])
    except CalledProcessError:
        return None

    return path


def get_gdrive_contents(url: str) -> t.Optional[str]:
    path = get_file_handle(url)

    if not path:
        return 'null'
    with open(path, 'r') as f:
        return f.read()


app = Flask(__name__)


@app.route('/<path:url>')
def file(url: str):
    return get_gdrive_contents(url or '')
