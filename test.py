from Services.ProximaREST import ProximaREST
from Settings.Manager import *
from Services.SqlAlc import *


sql = SqlAlc(get_sqlalchemy_dsn(), True)
api = ProximaREST(url=get_api_url(),
                  key=get_api_key(),
                  sign=get_api_sign(),
                  proxy=None,
                  wait_time=get_api_wait_time(),
                  retries=get_max_retries(),
                  timeout=get_timeout(),
                  threads=8)


def simple_test(*data):
    logger.info(f"Saved {list(map(len, data))} items.")


def save_hospitals(hospitals, addresses):
    try:
        sql.update_many_hospitals(hospitals)
        sql.update_many_addresses(addresses)
    except Exception as e:
        logging.error(e)


def save_persons(persons, specialists, jobs):
    sql.update_many_employees(persons)
    sql.update_many_specs(specialists)
    sql.update_many_jobs(jobs)


def save_posts(posts):
    sql.update_many_posts(posts)


def save_jobs(jobs):
    sql.update_many_jobs(jobs)


def save_types(types):
    sql.update_many_types(types)


def full_test():
    api.get_recent_hospitals(save_hospitals, 0, 24000, datetime.datetime(2021, 10, 1).timestamp())
    api.get_hospitals(save_hospitals, 0, 24000)
    api.get_recent_persons(save_persons, 0, 24000, datetime.datetime(2021, 10, 1).timestamp())
    api.get_persons(save_persons, 0, 24000)
    api.get_recent_jobs(save_jobs, 0, 24000, datetime.datetime(2021, 10, 1).timestamp())
    api.get_jobs(save_jobs, 0, 24000)
    api.get_types(save_types)
    api.get_posts(save_posts)
    logging.info("Testing is done.")


if __name__ == '__main__':
    api.get_persons(save_persons)



