"""Предоставляет доступ к функционалу API"""
import json
import threading
import time
import concurrent
from concurrent.futures import ThreadPoolExecutor
import requests as req

from Settings.Logger import *
from Services.ProximaHelpers import *
import Services.ProximaTemplates as Templates


class ProximaREST:
    """Предоставляет доступ к функционалу API"""

    def __init__(self, url, key, sign, proxy=None, wait_time=120, retries=0,
                 timeout=10, threads=8):
        self.url = url
        self.key = key
        self.sign = sign
        self.header = {'Token': str(self.key), 'sign': str(self.sign), 'Content-Type': 'application/json'}
        self.awaiting = None
        self.wait_time = wait_time
        self.proxy = proxy
        self.max_retries = retries
        self.timeout = timeout
        self.__parallel = True
        self.__retries = 0
        self.threads = threads
        self.__step = 1000  # Proxima не выдаёт более 1000 записей за один запрос

    @staticmethod
    def __success_msg(is_more, *data):
        return f"{'All data is gathered, r' if not is_more else 'R'}eceived " \
               f"{list(map(len, data))} items{', more available' if is_more else ''}."

    def __waiting_msg(self):
        return f"Data is not ready, waiting " \
               f"{f'{self.wait_time // 60}m ' if self.wait_time // 60 != 0 else ''}{self.wait_time % 60}s."

    def __retry(self, sleep=10, timeout_buff=0):
        time.sleep(sleep)
        self.__retries += 1
        self.timeout += timeout_buff
        return f"Trying to connect again. {self.max_retries - self.__retries} retries left."

    def __check_status(self, response, expected_item):
        info = response.json()

        if response.status_code == 201 and 'status' in info and info['status'] == 'IN_PROGRESS':
            self.awaiting = info["object_id"]
            raise WaitingException("Data is not ready.")

        if response.status_code != 200:
            raise CriticalException(f"Cannot get information. HTTP code {response.status_code}.")
        else:
            if 'getwait' in info and info['getwait']['result'][0]['status'] == 'IN_PROGRESS':
                self.awaiting = info['getwait']['result'][0]["object_id"]
                raise WaitingException("Data is not ready.")

            self.awaiting = None

            if 'resultcode' in info and info['resultcode'] == 400:
                raise Exception(f"API error: {info['errormessage']}.")

            if expected_item is not None and expected_item not in info:
                self.awaiting = None
                raise Exception(f"Cannot get information. [{info['resultcode']}] {info['errormessage']}.")

    def __send_request(self, template, method):
        """
        :param template: шаблон запроса из ProximaTemplates
        :param method: тип запроса из ProximaHelpers.ProximaMethods
        """
        try:
            if self.awaiting is None:
                r = req.post(self.url, data=template, headers=self.header, proxies=self.proxy, timeout=self.timeout)
                self.__check_status(r, method)
                data = r.json()[method]
            else:
                r = req.post(self.url,
                             data=Templates.awaited(self.awaiting),
                             headers=self.header,
                             proxies=self.proxy,
                             timeout=self.timeout)
                self.__check_status(r, None)
                data = r.json()[ProximaMethods.awaited()]["result"][0]["recordset"][method]
        except WaitingException:
            logger.debug(self.__waiting_msg())
            time.sleep(self.wait_time)
            return ResponsePayload(ProcessingStatus.WAITING)
        except CriticalException as e:
            logger.error(e)
            return ResponsePayload(ProcessingStatus.ERROR)
        except Exception as e:
            logger.error(e)
            if self.__retries >= self.max_retries:
                return ResponsePayload(ProcessingStatus.ERROR)
            else:
                logger.warning(self.__retry(timeout_buff=30))
                return ResponsePayload(ProcessingStatus.RETRY)

        self.__retries = 0
        return ResponsePayload(ProcessingStatus.OK, data)

    def __do_parallel(self, func, callback, skip, nmax, update=0):
        if nmax is None: step = int(15/self.threads + 1)*1000  # потоки будут выдавать от 1к до 8к элементов каждый
        else: step = round(nmax / self.threads)
        is_more = True
        self.__parallel = False

        while is_more:
            data = [[]]  # костыль, если убрать внешний цикл, будет вылетать во вложенной функции

            with ThreadPoolExecutor(max_workers=self.threads, thread_name_prefix="proxima") as executor:
                def save(*items):
                    if not data[0]:
                        for i in items: data[0].append(i)
                    else:
                        for d, i in zip(data[0], items): d += i

                future_to_request = {
                    executor.submit(func, save, skip + i*step, step, update): i for i in range(self.threads)
                }
                for future in concurrent.futures.as_completed(future_to_request):
                    is_more = is_more and future.result()

            logger.debug(f"Saving.")
            callback(*data[0])

            if not is_more or nmax is not None: break

        self.__parallel = True
        return is_more

    def get_recent_hospitals(self, callback, skip=0, nmax=None, update=0):
        """
        Список всех недавно обновлённых учреждений и их адресов\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Hospital], list[Address]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: время в формате timestamp последнего обновления записей
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """

        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_recent_hospitals, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        if nmax is not None: nmax += skip
        is_more = True
        count = [0, 0]

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            hosp, addr = [], []
            payload = self.__send_request(Templates.recent_hospitals(skip, self.__step, update),
                                          ProximaMethods.hospitals(True))
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                if row['get_orgs']['fetch'] == 0 or row["get_orgs"]["result"][0]["get_address"]["fetch"] == 0:
                    logger.warning(f"Received empty data: {row}")
                    continue
                obj = row["get_orgs"]["result"]

                hosp.append(Serializers.hospital_from_json(obj[0]))
                hosp[-1].update_time = datetime.datetime.fromtimestamp(row["last_update"])
                addr.append(Serializers.address_from_json(obj[0]["get_address"]["result"][0]))
                addr[-1].update_time = hosp[-1].update_time

            count = count[0] + len(hosp), count[1] + len(addr)
            if callback: callback(hosp, addr)
            logger.debug(ProximaREST.__success_msg(is_more, hosp, addr))
        return is_more

    def get_hospitals(self, callback, skip=0, nmax=None, update=None):
        """
        Список всех учреждений и их адресов\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Hospital], list[Address]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: не используется, нужен для совместимости с функцией многопоточности
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_hospitals, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        if nmax is not None: nmax += skip
        is_more = True
        count = [0, 0]

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            hosp, addr = [], []

            payload = self.__send_request(Templates.hospitals(skip, self.__step), ProximaMethods.hospitals())
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                if row["get_address"]["fetch"] == 0:
                    logger.warning(f"Received empty data: {row}")
                    continue

                hosp.append(Serializers.hospital_from_json(row))
                hosp[-1].update_time = datetime.datetime.now()
                addr.append(Serializers.address_from_json(row["get_address"]["result"][0]))
                addr[-1].update_time = datetime.datetime.now()

            count = count[0] + len(hosp), count[1] + len(addr)
            if callback: callback(hosp, addr)
            logger.debug(ProximaREST.__success_msg(is_more, hosp, addr))
        return is_more

    def get_recent_persons(self, callback, skip=0, nmax=None, update=0):
        """
        Список всех людей и их специальностей, недавно обновлённых в базе данных\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Person], list[Spec], list[Job]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: время в формате timestamp последнего обновления записей
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_recent_persons, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        is_more = True
        if nmax is not None: nmax += skip
        count = [0, 0, 0]

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            pers, specs, jobs = [], [], []

            payload = self.__send_request(Templates.recent_persons(skip, self.__step, update),
                                          ProximaMethods.persons(True))
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                obj = row["get_persons"]["result"]
                pers.append(Serializers.person_from_json(obj[0]))
                pers[-1].update_time = datetime.datetime.fromtimestamp(row["last_update"])

                if row["get_persons"]["result"][0]["get_spec"]["fetch"] != 0:
                    for s in obj[0]["get_spec"]["result"]:
                        specs.append(Serializers.spec_from_json(s))
                        specs[-1].update_time = pers[-1].update_time

                if row["get_persons"]["result"][0]["get_job"]["fetch"] != 0:
                    for j in obj[0]["get_job"]["result"]:
                        jobs.append(Serializers.job_from_json(j))
                        jobs[-1].update_time = pers[-1].update_time

            count = count[0] + len(pers), count[1] + len(specs), count[2] + len(jobs)
            if callback: callback(pers, specs, jobs)
            logger.debug(ProximaREST.__success_msg(is_more, pers, specs, jobs))

        return is_more

    def get_persons(self, callback, skip=0, nmax=None, update=None):
        """
        Список всех людей и их специальностей, недавно обновлённых в базе данных\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Person], list[Spec], list[Job]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: не используется, нужен для совместимости с функцией многопоточности
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_persons, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        is_more = True
        if nmax is not None: nmax += skip
        count = [0, 0, 0]

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            pers, specs, jobs = [], [], []

            payload = self.__send_request(Templates.persons(skip, self.__step), ProximaMethods.persons())
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                pers.append(Serializers.person_from_json(row))
                pers[-1].update_time = datetime.datetime.now()

                if row["get_spec"]["fetch"] != 0:
                    for s in row["get_spec"]["result"]:
                        specs.append(Serializers.spec_from_json(s))
                        specs[-1].update_time = datetime.datetime.now()

                if row["get_job"]["fetch"] != 0:
                    for j in row["get_job"]["result"]:
                        jobs.append(Serializers.job_from_json(j))
                        jobs[-1].update_time = datetime.datetime.now()

            count = count[0] + len(pers), count[1] + len(specs), count[2] + len(jobs)
            if callback: callback(pers, specs, jobs)
            logger.debug(ProximaREST.__success_msg(is_more, pers, specs, jobs))

        return is_more

    def get_recent_jobs(self, callback, skip=0, nmax=None, update=0):
        """
        Список всех мест работы из базы данных\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Job]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: время в формате timestamp последнего обновления записей
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_recent_jobs, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        is_more = True
        if nmax is not None: nmax += skip
        count = 0

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            jobs = []

            payload = self.__send_request(Templates.recent_jobs(skip, self.__step, update), ProximaMethods.jobs(True))
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                if row['get_job']['fetch'] == 0:
                    logger.warning(f"Received empty data: {row}")
                    continue
                obj = row["get_job"]["result"]
                j = Serializers.job_from_json(obj[0])
                j.update_time = datetime.datetime.fromtimestamp(row["last_update"])
                jobs.append(j)

            count += len(jobs)
            if callback: callback(jobs)
            logger.debug(ProximaREST.__success_msg(is_more, jobs))

        return is_more

    def get_jobs(self, callback, skip=0, nmax=None, update=None):
        """
        Список всех мест работы из базы данных\n
        Может работать в нескольких потоках, при nmax > 4000 и is_parallel\n
        :param callback: функция вида f(list[Job]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :param update: не используется, нужен для совместимости с функцией многопоточности
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if self.__parallel and self.threads > 1 and (nmax is None or nmax > 4000):
            logger.info("Requesting data.")
            return self.__do_parallel(self.get_jobs, callback, skip, nmax, update)
        elif self.threads == 1: logger.info("Requesting data.")
        else: logger.debug("Requesting data.")

        is_more = True
        if nmax is not None: nmax += skip
        logger.info("Requesting data.")
        count = 0

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            jobs = []

            payload = self.__send_request(Templates.jobs(skip, self.__step), ProximaMethods.jobs())
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                j = Serializers.job_from_json(row)
                j.update_time = datetime.datetime.now()
                jobs.append(j)

            count + len(jobs)
            if callback: callback(jobs)
            logger.debug(ProximaREST.__success_msg(is_more, jobs))

        return is_more

    def get_posts(self, callback, skip=0, nmax=None):
        """
        Список всех должностей из базы данных\n
        :param callback: функция вида f(list[Post]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if nmax is not None: nmax += skip
        is_more = True
        logger.info("Requesting data.")
        count = 0

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            posts = []

            payload = self.__send_request(Templates.posts(skip, self.__step), ProximaMethods.posts())
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                posts.append(Serializers.post_from_json(row))
                posts[-1].update_time = datetime.datetime.now()

            count += len(posts)
            if callback: callback(posts)
            logger.debug(ProximaREST.__success_msg(is_more, posts))

        return is_more

    def get_types(self, callback, skip=0, nmax=None):
        """
        Список всех типов учреждений\n
        :param callback: функция вида f(list[HospitalType]) -> None, которая будет вызываться для
            обработки/сохранения данных после каждого запроса
        :param skip: пропускает первые записи
        :param nmax: ограничение на общее число записей (всегда >= ProximaREST.step). None чтобы загружать до конца.
        :return: True, если остались незагруженные данные, False, если загруженны все данные
        """
        if nmax is not None: nmax += skip
        is_more = True
        logger.info("Requesting data.")
        count = 0

        while is_more and (nmax is None or nmax is not None and skip < nmax):
            types = []

            payload = self.__send_request(Templates.types(skip, self.__step), ProximaMethods.types())
            if payload.status == ProcessingStatus.ERROR or payload.status == ProcessingStatus.EMPTY: return is_more
            elif payload.status != ProcessingStatus.OK: continue

            is_more = payload.is_more
            skip += payload.fetch

            for row in payload.result:
                types.append(Serializers.type_from_json(row))
                types[-1].update_time = datetime.datetime.now()

            count += len(types)
            if callback: callback(types)
            logger.debug(ProximaREST.__success_msg(is_more, types))

        return is_more
