from datetime import datetime
from unittest import TestCase
import dss

from asdu.data_load.ft_history import FastToolsItem


class TestFastToolsItem(TestCase):
    def test_read_current_value(self):
        ft_item = FastToolsItem(item_name='TEST.test')
        ft_conn = dss.connect()
        ft_item.read_current_value(ft_conn=ft_conn)
        print(str(ft_item))
        dss.close(ft_conn)

    def test_read_current_values(self):
        items_list = [FastToolsItem(item_name='TEST.test'), FastToolsItem(item_name='TEST.TEST')]
        FastToolsItem.read_current_values(items_list)
        for ft_item in items_list:
            print("======\n", str(ft_item))

    def test_read_history_values(self):

        items_list = [
            FastToolsItem(item_name='ASDU_ESG.190.UNIT1.hour_24_aggr.finalValue', history_group='MASDU_ESG_AGGR')]
        FastToolsItem.read_history_values(items_list, time_stamp=datetime(2022, 20, 1, 22, 33))
        for ft_item in items_list:
            print("======\n", str(ft_item))

    def test_read_history(self):
        conn = dss.connect()

        ft_item = FastToolsItem(item_name='ASDU_ESG.BALANCE.UPTG.hour_24_aggr.finalValue', history_group='MASDU_ESG_AGGR')
        ft_item.read_history_aggregated_value(ft_conn=conn,
                                              time_stamp=datetime(year=2022, day=23, month=1, hour=9, minute=0),
                                              limit_in_seconds=60)
        print(str(ft_item), ft_item.time_stamp)
        dss.close(conn)

    # def test_read_history_value(self):
    #     self.fail()
    #
    # def test_read_current_values(self):
    #     self.fail()
    #
    # def test_read_history_values(self):
    #     self.fail()
