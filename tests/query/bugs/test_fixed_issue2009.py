import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestBugUpdateFilterOut(NebulaTestSuite):
    space = 'issue2009_default'
    tag = 'issue2009_default_tag'
    edge_type = 'issue2009_default_edge'

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
            f"CREATE TAG {TestBugUpdateFilterOut.tag}(id int DEFAULT 0, name string DEFAULT \'shylock\', age double DEFAULT 99.0, male bool DEFAULT true, birthday timestamp DEFAULT 0)"
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            f"CREATE EDGE {TestBugUpdateFilterOut.edge_type}(id int DEFAULT 0, name string DEFAULT \'shylock\', age double DEFAULT 99.0, male bool DEFAULT true, birthday timestamp DEFAULT 0)"
        )
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)

    # https://github.com/vesoft-inc/nebula/issues/2009
    def test_bugs_issue2009(self):
        # data
        resp = self.execute(
            f'INSERT VERTEX {TestBugUpdateFilterOut.tag}() VALUE {TestBugUpdateFilterOut.vertex}:()'
        )
        self.check_resp_succeeded(resp)
        resp = self.execute(
            f'INSERT EDGE {TestBugUpdateFilterOut.edge_type}() VALUE {TestBugUpdateFilterOut.vertex}->2333:()'
        )
        self.check_resp_succeeded(resp)
        # fetch
        resp = self.execute(
            f'FETCH PROP ON {TestBugUpdateFilterOut.tag} {TestBugUpdateFilterOut.vertex}'
        )
        self.check_resp_succeeded(resp)
        expect = [
            [TestBugUpdateFilterOut.vertex, 0, 'shylock', 99.0, True, 0]
        ]
        self.check_result(resp.rows, expect)

        resp = self.execute(
            f'FETCH PROP ON {TestBugUpdateFilterOut.edge_type} {TestBugUpdateFilterOut.vertex}->2333'
        )
        self.check_resp_succeeded(resp)
        expect = [
            [TestBugUpdateFilterOut.vertex, 2333, 0, 0, 'shylock', 99.0, True, 0]
        ]
        self.check_result(resp.rows, expect)

    @classmethod
    def cleanup(cls):
        print('Debug Point Clean Up')
        resp = cls.execute(f'DROP SPACE {TestBugUpdateFilterOut.space}')
        cls.check_resp_succeeded(resp)
