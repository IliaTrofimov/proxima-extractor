import pyodbc
from Settings.Logger import *
from Models.Hospital import *
from Models.Person import *


class SqlPy:
    """Предоставляет доступ к базе данных MS SQL Server\nИспользует pyodbc"""

    def __init__(self, dsn):
        self.__updated = 0
        self.__created = 0
        self.__errors = 0
        self.__skipped = 0
        self.__current_table = None

        try:
            self.cnxn = pyodbc.connect(dsn)
            logging.info("Connected to database")
        except pyodbc.DatabaseError as e:
            logging.error(e)
        self.cnxn.cursor().fast_executemany = True

    def manual(self, query, *args):
        """
        Выполняет произвольный запрос в БД\n
        :param query: запрос, указываейте <?>, чтобы потом вставить на эти места параметры *args
        """
        try:
            self.cnxn.cursor().execute(query, args)
            self.cnxn.commit()
        except pyodbc.DatabaseError as e:
            logging.error(e)

    def update(self, data, table):
        """
        Обновляет таблицу\nМожет работать долго при включённом VPN
        :param data: список объектов одного типа из Models.
        :param table: имя таблицы, которая будет изменяться.
        """

        if data is None or len(data) == 0:
            logging.error(f"Cannot update {table} with given data {type(data)}.")
            return None
        elif isinstance(data[0], Hospital):
            func = self.update_hospital
        elif isinstance(data[0], HospitalType):
            func = self.update_type
        elif isinstance(data[0], Address):
            func = self.update_address
        elif isinstance(data[0], Person):
            func = self.update_employee
        elif isinstance(data[0], Spec):
            func = self.update_spec
        elif isinstance(data[0], Job):
            func = self.update_job
        elif isinstance(data[0], Post):
            func = self.update_post
        else:
            logging.error(f"Cannot update {table} with given data {type(data)}.")
            return None

        self.__updated, self.__created, self.__errors, self.__skipped = 0, 0, 0, 0
        self.__current_table = table
        c = self.cnxn.cursor()

        for obj in data:
            try:
                if obj is not None:
                    func(obj, c)
            except pyodbc.Error as e:
                logging.error(f"'{obj}' (id {obj.uid}) caused an error in table {table}:")
                logging.error(e.args[1])
                self.__errors += 1
                if self.__errors > 5: break
        logging.info(
            f"{self.__created} created, {self.__updated} updated, {self.__skipped} skipped, {self.__errors} errors in {table}")

    def update_type(self, t, c):
        """
        :param t: HospitalType
        :param c pyodbc.Cursor
        """
        c.execute(f"SELECT uid FROM {self.__current_table} WHERE uid={t.uid}")
        if c.fetchone():
            c.execute(f"UPDATE {self.__current_table} SET parent_id=?,name=? WHERE uid=?",
                      t.parent_id, t.name, t.uid)
            self.__updated += 1
        else:
            c.execute(f"INSERT INTO {self.__current_table} (parent_id,name,uid) VALUES (?,?,?)",
                      t.parent_id, t.name, t.uid)
            self.__created += 1
        self.cnxn.commit()

    def update_address(self, a, c):
        """
        :param a: Address
        :param c pyodbc.Cursor
        """
        res = c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={a.uid}")
        if res:
            if res.update_time is None or a.update_time > res.update_time:
                c.execute(f"UPDATE {self.__current_table} SET country=?,area=?,region=?,city=?,street=?,"
                          "building=?,house=?,flat=? WHERE uid=?",
                          a.country, a.area, a.region, a.city, a.street,
                          a.building, a.house, a.flat, a.uid)
                self.__updated += 1
            else:
                self.__skipped += 1
        else:
            c.execute(f"INSERT INTO {self.__current_table} (country,area,region,city,street,building,house,flat,uid) "
                      "VALUES (?,?,?,?,?,?,?,?,?)",
                      a.country, a.area, a.region, a.city, a.street,
                      a.building, a.house, a.flat, a.uid)
            self.__created += 1
        self.cnxn.commit()

    def update_hospital(self, h, c):
        """
        :param h: Hospital
        :param c pyodbc.Cursor
        """
        c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={h.uid}")
        res = c.fetchone()

        if res:
            if res.update_time is None or h.update_time > res.update_time:
                c.execute(f"UPDATE {self.__current_table} SET name=?,type_id=?,property_form=?,is_archive=?,tax_code=?,"
                          "corp_id=?,center_id=?,phones=?,br_nick=?,status=?,update_time=? WHERE uid=?",
                          h.name, h.type_id, h.property_form, h.is_archive, h.tax_code, h.corp_id,
                          h.center_id, h.phones, h.br_nick, h.status, h.update_time, h.uid)
                self.__updated += 1
            else:
                self.__skipped += 1
        else:
            c.execute(f"INSERT INTO {self.__current_table} (name,type_id,property_form,is_archive,tax_code,"
                      "corp_id,center_id,phones,br_nick,status,update_time,uid) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                      h.name, h.type_id, h.property_form, h.is_archive, h.tax_code, h.corp_id,
                      h.center_id, h.phones, h.br_nick, h.status, h.update_time, h.uid)
            self.__created += 1
        self.cnxn.commit()

    def update_employee(self, e, c):
        """
        :param e: Person
        :param c pyodbc.Cursor
        """
        c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={e.uid}")
        res = c.fetchone()

        if res:
            if res.update_time is None or e.update_time > res.update_time:
                c.execute(
                    f"UPDATE {self.__current_table} SET firstname=?,lastname=?,secondname=?,is_archive=?,status=?,"
                    "sex=?,update_time=? WHERE uid=?",
                    e.firstname, e.lastname, e.secondname, e.is_archive, e.status, e.sex, e.update_time, e.uid)
                self.__updated += 1
            else:
                self.__skipped += 1
        else:
            c.execute(f"INSERT INTO {self.__current_table} (firstname,lastname,secondname,is_archive,status,"
                      "sex,update_time,uid) VALUES (?,?,?,?,?,?,?,?)",
                      e.firstname, e.lastname, e.secondname, e.is_archive, e.status, e.sex, e.update_time, e.uid)
            self.__created += 1
        self.cnxn.commit()

    def update_job(self, j, c):
        """
        :param j: Job
        :param c pyodbc.Cursor
        """
        c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={j.uid}")
        res = c.fetchone()

        if res:
            if res.update_time is None or j.update_time > res.update_time:
                c.execute(f"UPDATE {self.__current_table} SET person_id=?,is_main=?,is_archive=?,post_id=?,status=?,"
                          "org_id=?,update_time=? WHERE uid=?",
                          j.person_id, j.is_main, j.is_archive, j.post_id, j.status, j.org_id, j.update_time, j.uid)
                self.__updated += 1
            else:
                c.execute(f"INSERT INTO {self.__current_table} (person_id,is_main,is_archive,post_id,status,"
                          "org_id,update_time,uid) VALUES (?,?,?,?,?,?,?,?)",
                          j.person_id, j.is_main, j.is_archive, j.post_id, j.status, j.org_id, j.update_time, j.uid)
                self.__created += 1
        else:
            self.__skipped += 1
        self.cnxn.commit()

    def update_spec(self, s, c):
        """
        :param s: Spec
        :param c pyodbc.Cursor
         """
        c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={s.uid}")
        res = c.fetchone()

        if res:
            if res.update_time is None or s.update_time > res.update_time:
                c.execute(f"UPDATE {self.__current_table} SET name=?,category=?,is_main=?,update_time=? WHERE uid=?",
                          s.name, s.category, s.is_main, s.update_time, s.uid)
                self.__updated += 1
            else:
                self.__skipped += 1
        else:
            c.execute(
                f"INSERT INTO {self.__current_table} (name,category,is_main,update_time,uid) VALUES (?,?,?,?,?)",
                s.name, s.category, s.is_main, s.update_time, s.uid)
            self.__created += 1
        self.cnxn.commit()

    def update_post(self, p, c):
        """
        :param p: Post
        :param c pyodbc.Cursor
        """
        res = c.execute(f"SELECT uid, update_time FROM {self.__current_table} WHERE uid={p.uid}")
        if res:
            if res.update_time is None or p.update_time > res.update_time:
                c.execute(f"UPDATE {self.__current_table} SET name=?,type=? WHERE uid=?", p.name, p.type, p.uid)
                self.__updated += 1
            else:
                self.__skipped += 1
        else:
            c.execute(f"INSERT INTO {self.__current_table} (name,type,uid) VALUES (?,?,?)", p.name, p.type, p.uid)
            self.__created += 1
        self.cnxn.commit()

    # update_many_ используют pyodbc.executemany, поэтому более быстрые, но могут не работать в некоторых СУБД
    # всегда перезаписывают поля

    def update_many_types(self, types, table="HospitalTypes"):
        """
        Более быстрая альтернатива для update_types. Может не работать на Linux!
        :param types: list[HospitalType]
        :param table: str - название таблицы
        """
        data = list(t.to_tuple() * 2 for t in types)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET parent_id=?,name=? WHERE uid=? "
                f"IF @@ROWCOUNT=0 INSERT {table} (parent_id,name,uid) VALUES (?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_addresses(self, addresses, table="Addresses"):
        """
        Более быстрая альтернатива для update_address. Может не работать на Linux!
        :param addresses: list[Address]
        :param table: str - название таблицы
        """
        data = list(a.to_tuple() * 2 for a in addresses)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET country=?,area=?,region=?,city=?,street=?,building=?,house=?,flat=? WHERE uid=? "
                "IF @@ROWCOUNT=0 "
                f"INSERT {table} (country,area,region,city,street,building,house,flat,uid) VALUES (?,?,?,?,?,?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_hospitals(self, hospitals, table="Hospitals"):
        """
        Более быстрая альтернатива для update_hospital. Может не работать на Linux!
        :param hospitals: list[Hospital]
        :param table: str - название таблицы
        """
        data = list(h.to_tuple() * 2 for h in hospitals)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET name=?,type_id=?,property_form=?,is_archive=?,tax_code=?,"
                "corp_id=?,center_id=?,phones=?,br_nick=?,status=? WHERE uid=? IF @@ROWCOUNT=0 "
                f"INSERT {table} (name,type_id,property_form,is_archive,tax_code,corp_id,center_id,"
                "phones,br_nick,status,uid) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_employees(self, persons, table="Employee"):
        """
        Более быстрая альтернатива для update_employee. Может не работать на Linux!
        :param persons: list[Person]
        :param table: str - название таблицы
        """
        data = list(p.to_tuple() * 2 for p in persons)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET firstname=?,lastname=?,secondname=?,is_archive=?,status=?,sex=?,"
                "update_time=? WHERE uid=? IF @@ROWCOUNT=0 "
                f"INSERT INTO {table} (firstname,lastname,secondname,is_archive,status,"
                "sex,update_time,uid) VALUES (?,?,?,?,?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_jobs(self, jobs, table="Jobs"):
        """
        Более быстрая альтернатива для update_jobs. Может не работать на Linux!
        :param jobs: list[Job]
        :param table: str - название таблицы
        """
        data = list(j.to_tuple() * 2 for j in jobs)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET person_id=?,is_main=?,is_archive=?,post_id=?,status=?,"
                "org_id=?,update_time=? WHERE uid=? IF @@ROWCOUNT=0 "
                f"INSERT INTO {table} (person_id,is_main,is_archive,post_id,status,"
                "org_id,update_time,uid) VALUES (?,?,?,?,?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_specs(self, specs, table="Specialists"):
        """
        Более быстрая альтернатива для update_spec. Может не работать на Linux!
        :param specs: list[Spec]
        :param table: str - название таблицы
        """
        data = list(s.to_tuple() * 2 for s in specs)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET name=?,category=?,is_main=?,update_time=? WHERE uid=? "
                "IF @@ROWCOUNT=0 "
                f"INSERT INTO {table} (name,category,is_main,update_time,uid) VALUES (?,?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def update_many_posts(self, posts, table="Posts"):
        """
        Более быстрая альтернатива для update_post. Может не работать на Linux!
        :param posts: list[Posts]
        :param table: str - название таблицы
        """
        data = list(p.to_tuple() * 2 for p in posts)
        if data is not None and len(data) != 0:
            c = self.cnxn.cursor()
            c.fast_executemany = True
            c.executemany(
                f"UPDATE {table} SET name=?,type=?,update_time=? WHERE uid=? IF @@ROWCOUNT=0 "
                f"INSERT INTO {table} (name,type,update_time,uid) VALUES (?,?,?,?)",
                data)
            c.commit()
            logging.info("Update is done")
        else:
            logging.info("Cannot update empty data")

    def count_items(self, table):
        c = self.cnxn.cursor()
        result = c.execute(f"SELECT count(*) FROM {table}")
        try:
            return result.fetchone()[0]
        except Exception as e:
            logging.error(e)
