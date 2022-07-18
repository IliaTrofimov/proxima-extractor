import argparse
import json
import datetime
import logging


args_parser = argparse.ArgumentParser()
args_parser.add_argument('--sev', help=f'Logging severity level, can be "DEBUG", "INFO", "WARN". '
                                       f'Script uses value in config by default')


def get_proxies():
    with open("Settings/config.json") as f:
        data = json.load(f)
        return dict(data["PROXIES"])


def get_api_key():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["KEY"]


def get_api_sign():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["SIGN"]


def get_api_url():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["URL"]


def get_api_wait_time():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["WAIT_TIME"]


def get_sql_dsn():
    with open("Settings/config.json") as f:
        return json.load(f)["SQL"]["DSN"]


def get_sqlalchemy_dsn():
    with open("Settings/config.json") as f:
        return json.load(f)["SQL"]["DSN_SqlAlchemy"]


def get_update_time():
    with open("Settings/config.json") as f:
        return json.load(f)["RUNTIME"]["UPDATE"]


def get_max_retries():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["MAX_RETRIES"]


def get_timeout():
    with open("Settings/config.json") as f:
        return json.load(f)["API"]["TIMEOUT"]


def get_parallel():
    with open("Settings/config.json") as f:
        return int(json.load(f)["API"]["THREADS"])


def reset_update_time(time=None):
    with open("Settings/config.json", "r") as f:
        data = json.load(f)
    data["RUNTIME"]["UPDATE"] = int(datetime.datetime.now().timestamp()) if time is None else time
    with open("Settings/config.json", "w") as f:
        json.dump(data, f, indent=2)


def convert_time(timestamp):
    return str(datetime.datetime.fromtimestamp(timestamp))


def get_severity():
    with open("Settings/config.json") as f:
        lvl = json.load(f)["SEVERITY_LVL"]
        return severity_from_str(lvl)


def severity_from_str(lvl):
    if str.upper(lvl) == "DEBUG":
        return logging.DEBUG
    elif str.upper(lvl) == "INFO":
        return logging.INFO
    elif str.upper(lvl) == "WARN" or str.upper(lvl) == "WARNING":
        return logging.WARNING
    else:
        return logging.ERROR
