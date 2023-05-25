from __future__ import annotations
from datetime import datetime
import dss
import pytz
from dateutil.relativedelta import relativedelta
import logging


class FastToolsItem:
    def __init__(self, item_name: str = 'noname',
                 time_stamp: datetime = datetime.now().astimezone(),
                 history_group='ASDU',
                 rv_history_group='MASDU_ESG_RV'):
        self.item_name = item_name
        self.time_stamp = time_stamp
        self.value = 0.0
        self.aggr_history_group = history_group
        self.rv_history_group = rv_history_group

    def __str__(self):
        return 'name = ' + self.item_name + \
               ' value = ' + str(self.value)

    def read_current_value(self, ft_conn, value_dataset):
        """
        read current item value from Yokogawa FAST/TOOLS

        :param ft_conn: dss connection
        :param value_dataset: opened ITEM_VAL dataset
        :return: None
        """
        item_record = dss.readEqual(ft_conn, value_dataset, self.item_name)
        self.time_stamp = datetime.utcfromtimestamp(item_record['UPDATE_TIME'])
        self.value = item_record['ITEM_VALUE']

    def __read_history_value(self, ft_conn, time_stamp: datetime, history_group: str, limit_in_seconds: int = 1):
        """
        reads item value from given history_group at given timestamp
        :param ft_conn: dss connection instance
        :param time_stamp: read history timestamp
        :param history_group: item historisation group
        :param limit_in_seconds: +- time interval around timestamp to get the value
        :return: None
        """
        start_time = (time_stamp.astimezone(pytz.timezone("GMT")) - relativedelta(seconds=limit_in_seconds)).timestamp()
        end_time = (time_stamp.astimezone(pytz.timezone("GMT")) + relativedelta(seconds=limit_in_seconds)).timestamp()

        history_records = list(
            dss.getItemHistory(ft_conn, self.item_name, history_group, start_time, end_time, 1))

        if len(history_records) > 0 and history_records[0] is not None:
            self.time_stamp = datetime.fromtimestamp(history_records[0][0])
            self.value = history_records[0][1]
        else:
            self.time_stamp = time_stamp

    def read_history_rv_value(self, ft_conn, time_stamp: datetime):
        """
        reads value from realtime historisation group with 1 second precision.
        :param ft_conn: dss connection instance
        :param time_stamp:
        :return: None
        """
        return self.__read_history_value(ft_conn, time_stamp, self.rv_history_group, limit_in_seconds=1)

    def read_history_aggregated_value(self, ft_conn, time_stamp: datetime, limit_in_seconds=1):
        """
        reads value from aggregated values historisation group.
        :param ft_conn: dss connection instance
        :param time_stamp:
        :param limit_in_seconds: +- time interval around timestamp to get the value
        :return:
        """
        return self.__read_history_value(ft_conn, time_stamp, self.aggr_history_group, limit_in_seconds)

    @classmethod
    def read_current_values(cls, items: list[FastToolsItem]):
        """
        reads the values for each FastToolsItem in the given list of items and
        updates their values with values from the history group rv_history_group
        :param items: list of items for reading
        :return: None
        """
        conn = dss.connect()
        for item in items:
            try:
                item.read_current_value(ft_conn=conn)
            except BaseException as ex:
                logging.error(f"Read {item.item_name} from {item.rv_history_group}  error:{ex}")
        dss.close(conn)

    @classmethod
    def read_history_values(cls, items: list[FastToolsItem], time_stamp: datetime):
        """
        reads the values for each FastToolsItem in the given list of items and
        updates their values with values from the history group rv_history_group
        with one second precision
        :param items: list of items for reading
        :param time_stamp: timestamp for reading
        :return: None
        """
        if time_stamp is None:
            time_stamp = datetime.now().astimezone()
        conn = dss.connect()
        for item in items:
            try:
                item.read_history_aggregated_value(ft_conn=conn, time_stamp=time_stamp)
            except BaseException as ex:
                logging.error(f"Read {item.item_name} from {item.aggr_history_group}  error:{ex}")
        dss.close(conn)
