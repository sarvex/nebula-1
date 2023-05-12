import time

import pytest

from tests.common.nebula_test_suite import NebulaTestSuite


@pytest.mark.parametrize('schema', [('TAG', 'VERTEX', '233'), ('EDGE', 'EDGE', '233->2333')])
class TestBugUpdateFilterOut(NebulaTestSuite):
    space = 'issue1987_update'

    @classmethod
    def prepare(cls):
        resp = cls.execute(f'DROP SPACE IF EXISTS {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
        resp = cls.execute(f'CREATE SPACE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        resp = cls.execute(f'USE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)

    # https://github.com/vesoft-inc/nebula/issues/1987
    def test_bugs_issue1987(self, schema):
        # schema
        resp = self.execute('DROP TAG IF EXISTS t')
        self.check_resp_succeeded(resp)
        resp = self.execute('DROP EDGE IF EXISTS t')
        self.check_resp_succeeded(resp)
        resp = self.execute(f'CREATE {schema[0]} t(id int, name string)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)

        # ADD default value
        resp = self.execute(
            f"ALTER {schema[0]} t CHANGE (id int DEFAULT 233, name string DEFAULT \'shylock\')"
        )
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(f'INSERT {schema[1]} t() values {schema[2]}:()')
        self.check_resp_succeeded(resp)
        resp = self.execute(f'FETCH PROP ON t {schema[2]}')
        self.check_resp_succeeded(resp)
        ignore = {0} if schema[0] == 'TAG' else {0, 1, 2}
        expect = [[233, 'shylock']]
        self.check_result(resp.rows, expect, ignore)

        # Change default value
        resp = self.execute(
            f"ALTER {schema[0]} t CHANGE (id int DEFAULT 444, name string DEFAULT \'hg\')"
        )
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(f'INSERT {schema[1]} t() values {schema[2]}:()')
        self.check_resp_succeeded(resp)
        resp = self.execute(f'FETCH PROP ON t {schema[2]}')
        self.check_resp_succeeded(resp)
        ignore = {0} if schema[0] == 'TAG' else {0, 1, 2}
        expect = [[444, 'hg']]
        self.check_result(resp.rows, expect, ignore)

        # Drop default value
        resp = self.execute(f'ALTER {schema[0]} t CHANGE (id int, name string)')
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
        resp = self.execute(f'INSERT {schema[1]} t() values {schema[2]}:()')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(cls):
        resp = cls.execute(f'DROP SPACE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
