"""
Класс организации и вспомогательные классы, предоставляющие дополнительную информацию о ней
"""

from Models.BaseModel import Base


class Hospital(Base):
    def __init__(self, name: str, uid: int, type_id: int,
                 property_form=None, status="Active", is_archive=0,
                 tax_code=None, corp_id=None, center_id=None, br_nick=None,
                 phones="", update_time=None):
        super().__init__(uid, update_time)
        self.name = name
        self.uid = int(uid)
        self.type_id = int(type_id)
        self.property_form = property_form
        self.status = status
        self.is_archive = int(is_archive)
        self.tax_code = tax_code
        self.corp_id = corp_id
        self.center_id = center_id
        self.br_nick = br_nick
        self.phones = phones

    def __repr__(self):
        return self.name

    def to_tuple(self):
        return (self.name, self.type_id, self.property_form, self.is_archive, self.tax_code,
                self.corp_id, self.center_id, self.phones, self.br_nick, self.status, self.uid)


class HospitalType(Base):
    def __init__(self, name: str, uid: int, parent_id: int, update_time=None):
        super().__init__(uid, update_time=update_time)
        self.name = name
        self.uid = uid
        self.parent_id = parent_id

    def __repr__(self):
        return self.name

    def to_tuple(self):
        return self.parent_id, self.name, self.uid


class Address(Base):
    def __init__(self, uid: int, country=None, area=None, region=None,
                 city=None, street=None, building=None,
                 house=None, flat=None, hosp_id=None, city_id=None,
                 update_time=None, type_street=None):
        super().__init__(uid, update_time=update_time)
        self.country = country
        self.area = area
        self.region = region
        self.city = city
        self.street = street
        self.building = building
        self.house = house
        self.flat = flat
        self.hosp_id = hosp_id
        self.city_id = city_id
        self.type_street = type_street

    def __repr__(self):
        return f"{self.city}, ул.{self.street}, д.{self.house}"

    def to_tuple(self):
        return (self.country, self.area, self.region, self.city,
                self.street, self.building, self.house, self.flat, self.uid)
