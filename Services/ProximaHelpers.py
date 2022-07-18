"""Вспомогательные функции и типы для ProximaREST"""
import datetime
from enum import Enum
from Models.Person import *
from Models.Hospital import *


class WaitingException(Exception):
    """Возникает, когда сервер сообщает, что даные ещё не готовы"""
    pass


class CriticalException(Exception):
    """Возникает, когда из-за ошибки нельзя продолжить работу"""
    pass


class ProcessingStatus(Enum):
    OK = 1
    WAITING = 2
    ERROR = 3
    RETRY = 4
    EMPTY = 5


class ResponsePayload:
    """Класс, хранящий полезную информацию об ответе: статус и данные для выгрузки"""

    def __init__(self, status=ProcessingStatus.EMPTY, data=None):
        self.status = status
        self.result = None
        self.fetch = 0
        self.is_more = False

        if data is None or data["fetch"] == 0:
            self.status = ProcessingStatus.EMPTY
        elif data is not None:
            self.result = data["result"]
            self.fetch = data["fetch"]
            self.is_more = data["more"]


class ProximaMethods:
    """Строки с именами методов для HTTP запросов к API"""

    @staticmethod
    def persons(recent=False):
        return "get_prepared_persons" if recent else "get_persons"

    @staticmethod
    def hospitals(recent=False):
        return "get_prepared_organizations" if recent else "get_orgs"

    @staticmethod
    def jobs(recent=False):
        return "get_prepared_job" if recent else "get_job"

    @staticmethod
    def posts():
        return "get_dict_post"

    @staticmethod
    def addresses():
        return "get_address"

    @staticmethod
    def phones():
        return "get_phoneorg"

    @staticmethod
    def types():
        return "get_typeorgs"

    @staticmethod
    def specialists():
        return "get_spec"

    @staticmethod
    def awaited():
        return "getwait"


class Serializers:
    """Содержит функции для преобразования JSON ответа в нужные классы"""
    @staticmethod
    def hospital_from_json(data):
        return Hospital(data["org_name"], data["org_id"], data["type_org_id"],
                        property_form=data["form_property_name"],
                        tax_code=data["code_tax"],
                        status=data["status"],
                        corp_id=data["corp_id"],
                        center_id=data["center_id"],
                        is_archive=data["is_archive"],
                        phones="" if not data["get_phoneorg"]["result"] else
                        ";".join(p["phone"] for p in data["get_phoneorg"]["result"][:5]),
                        br_nick=data["br_nick"],
                        update_time=datetime.datetime.now())

    @staticmethod
    def address_from_json(data):
        return Address(uid=data["object_id"],
                       country=data["country"],
                       area=data["area"],
                       region=data["region"],
                       city=data["city"],
                       street=data["street"],
                       building=data["building"],
                       house=data["house"],
                       flat=data["flat"],
                       city_id=data["city_id"],
                       update_time=datetime.datetime.now(),
                       type_street=data["type_street"])

    @staticmethod
    def job_from_json(data):
        return Job(data["post_id"],
                   data["person_id"],
                   data["object_id"],
                   org_id=data["org_id"],
                   is_archive=data["is_archive"],
                   is_main=data["is_main"],
                   status=data["status"],
                   update_time=datetime.datetime.now())

    @staticmethod
    def person_from_json(data):
        return Person(data["firstname"], data["lastname"],
                      data["secondname"], data["person_id"],
                      status=data["status"],
                      is_archive=data["is_archive"],
                      sex=data["sex"],
                      update_time=datetime.datetime.now())

    @staticmethod
    def spec_from_json(data):
        return Spec(data["name_rus"], data["object_id"], category=data["category"],
                    is_main=data["is_main"], update_time=datetime.datetime.now())

    @staticmethod
    def post_from_json(data):
        return Post(data["name_rus"], data["type_name"], data["object_id"], update_time=datetime.datetime.now())

    @staticmethod
    def type_from_json(data):
        return HospitalType(data["name"], data["object_id"], data["parent_id"], update_time=datetime.datetime.now())
