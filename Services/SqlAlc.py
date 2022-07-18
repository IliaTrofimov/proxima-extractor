"""
Sql Alchemy
"""

from sqlalchemy import create_engine, text
from Settings.Logger import *


class SqlAlc:
    def __init__(self, dsn, verbose=True):
        """
        :param dsn: connection string
        :param verbose: True чтобы выводить информацию об успешном завершении работы методов,
        False будет выводить только данные об ошибках
        """
        self.__engine = None
        self.verbose = verbose
        try:
            self.__engine = create_engine(dsn, fast_executemany=True)
        except Exception as e:
            logger.error(e)

    def update_many_types(self, types, table="HospitalTypes"):
        data = list(t.__dict__ for t in types)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(
                        f"MERGE {table} WITH (SERIALIZABLE) AS T USING (VALUES (:uid, :name, :parent_id, :update_time))"
                        " AS U (uid, name, parent_id, update_time) ON U.uid = T.uid "
                        "WHEN MATCHED THEN UPDATE SET T.name=U.name,T.parent_id=U.parent_id,T.update_time=U.update_time"
                        " WHEN NOT MATCHED THEN INSERT "
                        "(uid, name, parent_id, update_time) VALUES (U.uid, U.name, U.parent_id, U.update_time);"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_addresses(self, addresses, table="Addresses"):
        data = list(t.__dict__ for t in addresses)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"UPDATE {table} SET country=:country,area=:area,region=:region,city=:city,city_id=:city_id,"
                         "street=:street,building=:building,house=:house,flat=:flat,update_time=:update_time,"
                         "type_street=:type_street WHERE uid=:uid "
                         "IF @@ROWCOUNT=0 "
                         f"INSERT {table} (country,area,region,city,city_id,type_street,street,building,house,flat,uid)"
                         " VALUES (:country,:area,:region,:city,:city_id,:type_street,:street,:building,:house,:flat,:uid)"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_hospitals(self, hospitals, table="Hospitals"):
        data = list(t.__dict__ for t in hospitals)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"MERGE {table} WITH (SERIALIZABLE) AS T USING (VALUES "
                         "(:uid,:name,:type_id,:property_form,:is_archive,:tax_code,:corp_id,:center_id,:phones,"
                         ":br_nick,:status,:update_time)) "
                         "AS U (uid,name,type_id,property_form,is_archive,tax_code,corp_id,center_id,phones,"
                         "br_nick,status,update_time) ON U.uid = T.uid "
                         "WHEN MATCHED THEN UPDATE SET T.name=U.name, T.type_id=U.type_id, T.property_form=U.property_form,"
                         "T.is_archive=U.is_archive,T.tax_code=U.tax_code,T.corp_id=U.corp_id,T.center_id=U.center_id,"
                         "T.phones=U.phones,T.br_nick=U.br_nick,T.status=U.status,T.update_time=U.update_time"
                         " WHEN NOT MATCHED THEN INSERT "
                         "(uid,name,type_id,property_form,is_archive,tax_code,corp_id,center_id,phones,br_nick,status,update_time) "
                         "VALUES (U.uid,U.name,U.type_id,U.property_form,U.is_archive,U.tax_code,U.corp_id,U.center_id,"
                         "U.phones,U.br_nick,U.status,U.update_time);"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_employees(self, persons, table="Employee"):
        data = list(t.__dict__ for t in persons)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"UPDATE {table} SET firstname=:firstname,lastname=:lastname,secondname=:secondname,"
                         "is_archive=:is_archive,status=:status,sex=:sex,update_time=:update_time "
                         "WHERE uid=:uid IF @@ROWCOUNT=0 "
                         f"INSERT INTO {table} (firstname,lastname,secondname,is_archive,status,sex,update_time,uid) "
                         "VALUES (:firstname,:lastname,:secondname,:is_archive,:status,:sex,:update_time,:uid)"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_jobs(self, jobs, table="Jobs"):
        data = list(t.__dict__ for t in jobs)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"UPDATE {table} SET person_id=:person_id,is_main=:is_main,is_archive=:is_archive,"
                         "post_id=:post_id,status=:status,org_id=:org_id,update_time=:update_time "
                         "WHERE uid=:uid IF @@ROWCOUNT=0 "
                         f"INSERT INTO {table} (person_id,is_main,is_archive,post_id,status,org_id,update_time,uid) "
                         "VALUES (:person_id,:is_main,:is_archive,:post_id,:status,:org_id,:update_time,:uid)"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_specs(self, specs, table="Specialists"):
        data = list(t.__dict__ for t in specs)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"UPDATE {table} SET name=:name,category=:category,is_main=:is_main,update_time=:update_time "
                         "WHERE uid=:uid IF @@ROWCOUNT=0 "
                         f"INSERT INTO {table} (name,category,is_main,update_time,uid) "
                         "VALUES (:name,:category,:is_main,:update_time,:uid)"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def update_many_posts(self, posts, table="Posts"):
        data = list(t.__dict__ for t in posts)
        if data is not None and len(data) != 0:
            with self.__engine.begin() as c:
                if self.verbose: count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0]
                c.execute(
                    text(f"UPDATE {table} SET name=:name,type=:type,update_time=:update_time WHERE uid=:uid "
                         f"IF @@ROWCOUNT=0 "
                         f"INSERT INTO {table} (name,type,update_time,uid) VALUES (:name,:type,:update_time,:uid)"),
                    data)
                if self.verbose:
                    count = c.execute(text(f"SELECT count(*) FROM {table}")).fetchone()[0] - count
                    logger.info(f"Update is done (inpt: {len(data)}, cnng: {count:+d})")
        else:
            logger.warning("Cannot update empty data")

    def count_items(self, table):
        with self.__engine.begin() as c:
            result = c.execute(text(f"SELECT count(*) FROM {table}"))
            try:
                return result.fetchone()[0]
            except Exception as e:
                logger.error(e)
