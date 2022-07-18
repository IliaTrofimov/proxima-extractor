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
                  threads=get_parallel())


def save_hospitals(hospitals, addresses):
    try:
        sql.update_many_hospitals(hospitals)
        sql.update_many_addresses(addresses)
    except Exception as e:
        logger.error(e)


def save_persons(persons, specialists, jobs):
    try:
        sql.update_many_employees(persons)
        sql.update_many_specs(specialists)
        sql.update_many_jobs(jobs)
    except Exception as e:
        logger.error(e)


def save_posts(posts):
    try:
        sql.update_many_posts(posts)
    except Exception as e:
        logger.error(e)


def save_jobs(jobs):
    try:
        sql.update_many_jobs(jobs)
    except Exception as e:
        logger.error(e)


def save_types(types):
    try:
        sql.update_many_types(types)
    except Exception as e:
        logger.error(e)


if __name__ == "__main__":
    last_update = get_update_time()
    logger.info(f"Launching, last update {convert_time(last_update)}.")

    if sql.count_items("Employee") == 0 or sql.count_items("Jobs") == 0 or sql.count_items("Specialists") == 0:
        logger.info("Employee/Jobs/Specialists is empty, repopulating it.")
        api.get_persons(save_persons, nmax=600000)
    else:
        api.get_recent_persons(save_persons, nmax=30000, update=last_update)

    if sql.count_items("Addresses") == 0 or sql.count_items("Hospitals") == 0:
        logger.info("Addresses/Hospitals is empty, repopulating it.")
        api.get_hospitals(save_hospitals, nmax=400000)
    else:
        api.get_recent_hospitals(save_hospitals, nmax=30000, update=last_update)

    api.get_types(save_types, nmax=1000)
    api.get_posts(save_posts, nmax=5000)

    reset_update_time()
    logger.info("Done.")
