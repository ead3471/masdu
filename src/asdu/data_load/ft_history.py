from __future__ import annotations
from datetime import datetime
import dss
import logging

import pytz
from dateutil.relativedelta import relativedelta


class FastToolsItem:

    def __init__(self, item_name: str = 'noname', time_stamp: datetime = datetime.now().astimezone(),
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
        item_record = dss.readEqual(ft_conn, value_dataset, self.item_name)
        self.time_stamp = datetime.utcfromtimestamp(item_record['UPDATE_TIME'])
        self.value = item_record['ITEM_VALUE']

    def read_history_value(self, ft_conn, time_stamp, history_group: str, limit_in_seconds=1):

        # start_time_str = (time_stamp.astimezone() - relativedelta(seconds=limit_in_seconds)).strftime(
        #      "%d-%m-%Y %H:%M:%S")
        # end_time_str = (time_stamp.astimezone() + relativedelta(seconds=limit_in_seconds)).strftime("%d-%m-%Y %H:%M:%S")
        # start_time = dss.dateConvert(ft_conn, dss.dateDSS(ft_conn, start_time_str), "LCT_TO_GMT")
        # end_time = dss.dateConvert(ft_conn, dss.dateDSS(ft_conn, end_time_str), "LCT_TO_GMT")
        # print(time_stamp.astimezone(pytz.timezone("GMT")).timestamp())
        # print(dss_start_time,dss_end_time)

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
        return self.read_history_value(ft_conn, time_stamp, self.rv_history_group, limit_in_seconds=1)

    def read_history_aggregated_value(self, ft_conn, time_stamp: datetime, limit_in_seconds=1):
        return self.read_history_value(ft_conn, time_stamp, self.aggr_history_group, limit_in_seconds)

    @classmethod
    def read_current_values(cls, items: list[FastToolsItem]):
        conn = dss.connect()
        for item in items:
            try:
                item.read_current_value(ft_conn=conn)
            except BaseException as ex:
                # TODO: catch exception to log
                print(str(ex))
        dss.close(conn)

    @classmethod
    def read_history_values(cls, items: list[FastToolsItem], time_stamp: datetime):
        if time_stamp is None:
            time_stamp = datetime.now().astimezone()
        conn = dss.connect()
        for item in items:
            try:
                item.read_history_aggregated_value(ft_conn=conn, time_stamp=time_stamp)
            except BaseException as ex:
                # TODO: catch exception to log
                print(str(ex))
        dss.close(conn)
