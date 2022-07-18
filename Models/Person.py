"""Класс, пердставляющий человека, и вспомогательные классы, описывающие дополнительные свойства сотрудников"""

from Models.BaseModel import Base


class Person(Base):
    """Представляет одного человека"""

    def __init__(self, firstname: str, lastname: str, secondname: str, uid: int,
                 update_time=None, status="Активный", is_archive=0, sex=1):
        super().__init__(uid, update_time)
        self.firstname = firstname
        self.lastname = lastname
        self.secondname = secondname
        self.uid = uid
        self.status = status
        self.is_archive = is_archive
        self.sex = sex

    def __repr__(self):
        return f"{self.lastname} {self.firstname[0]}.{self.secondname[0]}."

    def to_tuple(self):
        return (self.firstname, self.lastname, self.secondname, self.is_archive, self.status,
                self.sex, self.update_time, self.uid)


class Spec(Base):
    """Представляет информацию о специальности одного человека Person"""

    def __init__(self, name: str, uid: int,
                 category=0, is_main=1, update_time=None):
        super().__init__(uid, update_time)
        self.name = name
        self.category = category
        self.is_main = is_main

    def __repr__(self):
        return self.name

    def to_tuple(self):
        return self.name, self.category, self.is_main, self.update_time, self.uid


class Post(Base):
    """Представляет информацию о должности в конкретной организации Hospital"""

    def __init__(self, name: str, type_name: str, uid: int, update_time=None):
        super().__init__(uid, update_time=update_time)
        self.name = name
        self.uid = uid
        self.type = type_name

    def __repr__(self):
        return self.name

    def to_tuple(self):
        return self.name, self.type, self.update_time, self.uid


class Job(Base):
    """Представляет информацию о месте работы человека Person в органицазии Hospital,
    соответсвующей должности Post"""

    def __init__(self, post_id: int, person_id: int, uid: int,
                 org_id=None, is_main=1, status="Активный",
                 is_archive=0, update_time=None):
        super().__init__(uid, update_time)
        self.person_id = person_id
        self.is_main = is_main
        self.is_archive = is_archive
        self.post_id = post_id
        self.status = status
        self.org_id = org_id

    def to_tuple(self):
        return (self.person_id, self.is_main, self.is_archive, self.post_id, self.status,
                self.org_id, self.update_time, self.uid)
