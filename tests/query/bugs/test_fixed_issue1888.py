import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite

@pytest.mark.parametrize('sentence', ['UPDATE', 'UPSERT'])
class TestBugUpdateFilterOut(NebulaTestSuite):
    space      = 'issue1888_update'
    tag        = 'issue1888_update_tag'
    edge_type  = 'issue1888_update_edge'

    vertex = 233

    @classmethod
    def prepare(cls):
        resp = cls.execute(f'CREATE SPACE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        resp = cls.execute(f'USE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
        # schema
        resp = cls.execute(
            f'CREATE TAG {TestBugUpdateFilterOut.tag}(id int, name string)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            f'CREATE EDGE {TestBugUpdateFilterOut.edge_type}(id int, name string)'
        )
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        # data
        resp = cls.execute(
            f'INSERT VERTEX {TestBugUpdateFilterOut.tag}(id, name) VALUE {TestBugUpdateFilterOut.vertex}:(0, "shylock")'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            f'INSERT EDGE {TestBugUpdateFilterOut.edge_type}(id, name) VALUE {TestBugUpdateFilterOut.vertex}->2333:(0, "shylock")'
        )
        cls.check_resp_succeeded(resp)

    # https://github.com/vesoft-inc/nebula/issues/1888
    def test_bugs_issue1888(self, sentence):
        # update vertex filter out
        resp = self.execute(
            f'{sentence} VERTEX {TestBugUpdateFilterOut.vertex} SET {TestBugUpdateFilterOut.tag}.name = "hg" WHEN $^.{TestBugUpdateFilterOut.tag}.id > 0'
        )
        self.check_resp_succeeded(resp)
        resp = self.execute(
            f'FETCH PROP ON {TestBugUpdateFilterOut.tag} {TestBugUpdateFilterOut.vertex}'
        )
        self.check_resp_succeeded(resp)
        expect = [[TestBugUpdateFilterOut.vertex, 0, 'shylock']]
        self.check_result(resp.rows, expect)

        # update edge filter out
        resp = self.execute(
            f'{sentence} EDGE {TestBugUpdateFilterOut.vertex}->2333 OF {TestBugUpdateFilterOut.edge_type} SET name = "hg" WHEN {TestBugUpdateFilterOut.edge_type}.id > 0'
        )
        self.check_resp_succeeded(resp)
        resp = self.execute(
            f'FETCH PROP ON {TestBugUpdateFilterOut.edge_type} {TestBugUpdateFilterOut.vertex}->2333'
        )
        self.check_resp_succeeded(resp)
        expect = [[TestBugUpdateFilterOut.vertex, 2333, 0, 0, 'shylock']]
        self.check_result(resp.rows, expect)


    @classmethod
    def cleanup(cls):
        print('Debug Point Clean Up')
        resp = cls.execute(f'DROP SPACE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
