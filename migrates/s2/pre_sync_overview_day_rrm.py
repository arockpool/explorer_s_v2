import datetime, os, sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(BASE_DIR))
from bson.decimal128 import Decimal128
from base.utils.fil import _d
from explorer.models.overview import OverviewDay
import csv


class PreProcess:
    def __init__(self, app):
        self.process_data(app)

    @classmethod
    def process_data(cls, app):
        with open("overview_overviewday.csv") as f:
            f_csv = csv.reader(f)
            for rows in f_csv:
                date = datetime.datetime.strptime(rows[0], "%d/%m/%Y").strftime("%Y-%m-%d")
                OverviewDay.objects(date=date).update(
                    price=Decimal128(rows[1]),
                )