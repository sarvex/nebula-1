import time
import pytest
from graph import ttypes
from tests.common.nebula_test_suite import NebulaTestSuite
from tests.bench.data_generate import insert_vertices, insert_edges


class TestDeleteBench(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        resp = cls.execute(
            'CREATE SPACE IF NOT EXISTS benchdeletespace(partition_num={partition_num}, replica_factor={replica_factor})'.format(
                partition_num=cls.partition_num, replica_factor=cls.replica_factor
            )
        )
        cls.check_resp_succeeded(resp)
        time.sleep(4)
        resp = cls.execute('USE benchdeletespace')
        time.sleep(4)
        cls.check_resp_succeeded(resp)
        resp = cls.execute('CREATE TAG IF NOT EXISTS person(name string, age int)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('CREATE TAG index IF NOT EXISTS personName on person(name)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('CREATE TAG index IF NOT EXISTS personAge on person(age)')
        cls.check_resp_succeeded(resp)
        time.sleep(4)
        cls.execute('CREATE EDGE IF NOT EXISTS like(likeness int)')
        cls.check_resp_succeeded(resp)
        time.sleep(4)
        insert_vertices(cls, "benchdeletespace", 50, 20000)
        insert_edges(cls, "benchdeletespace", 50, 20000)

    @classmethod
    def cleanup(cls):
        pass
        # resp = self.execute('drop space benchdeletespace')
        # self.check_resp_succeeded(resp)

    def delete(self, duration=0.000001):
        for i in range(10000):
            query = 'lookup on person where person.age == {0} '.format(i)
            resp = self.execute(query)
            self.check_resp_succeeded(resp)
            if resp.rows is not None:
                for row in resp.rows:
                    for col in row.columns:
                        if col.getType() == ttypes.ColumnValue.ID:
                            id = col.get_id()
                            resp = self.execute('DELETE VERTEX {0} WITH EDGE'.format(id))
                            self.check_resp_succeeded(resp)

            resp = self.execute('lookup on person where person.age == {0} '.format(i))
            assert resp.rows is None, f"Error Query: {query}"
        return ""

    @pytest.mark.benchmark(
        group="test_delete",
        min_time=0.1,
        max_time=0.5,
        min_rounds=1,
        timer=time.time,
        disable_gc=True,
        warmup=False
    )
    def test_delete(self, benchmark):
        benchmark(self.delete)
