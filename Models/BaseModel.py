import datetime


class Base:
    def __init__(self, uid: int, update_time=None):
        self.update_time = update_time if update_time is not None else datetime.datetime.now()
        self.uid = uid

    def __eq__(self, other):
        return isinstance(other, Base) and other.uid == self.uid and self.update_time == other.update_time

    def __repr__(self):
        return f"obj. {self.uid}"
