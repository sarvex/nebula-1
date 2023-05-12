# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestSimpleQuery(NebulaTestSuite):

    @classmethod
    def prepare(cls):
        resp = cls.execute(
            'CREATE SPACE IF NOT EXISTS fixed_pr_2042(partition_num={partition_num}, replica_factor={replica_factor})'.format(
                partition_num=cls.partition_num, replica_factor=cls.replica_factor
            )
        )
        cls.check_resp_succeeded(resp)

        resp = cls.execute('USE fixed_pr_2042')
        cls.check_resp_succeeded(resp)

        resp = cls.execute('CREATE TAG IF NOT EXISTS person()')
        cls.check_resp_succeeded(resp)

        resp = cls.execute('CREATE EDGE IF NOT EXISTS relation()')
        cls.check_resp_succeeded(resp)

        time.sleep(cls.delay)

    def test_empty_input_in_fetch(self):
        resp = self.execute('GO FROM 11 over relation YIELD relation._dst as id | FETCH PROP ON person 11 YIELD $-.id')
        self.check_resp_failed(resp)

    @classmethod
    def cleanup(cls):
        resp = cls.execute('drop space fixed_pr_2042')
        cls.check_resp_succeeded(resp)
