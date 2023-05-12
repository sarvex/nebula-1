# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestSimpleQuery(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        resp = cls.execute(
            'CREATE SPACE IF NOT EXISTS fixed_over_all_pr1962(partition_num={partition_num}, replica_factor={replica_factor})'.format(
                partition_num=cls.partition_num, replica_factor=cls.replica_factor
            )
        )
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        resp = cls.execute('USE fixed_over_all_pr1962')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('CREATE TAG IF NOT EXISTS person(name string, age int)')
        cls.check_resp_succeeded(resp)

        resp = cls.execute('CREATE EDGE IF NOT EXISTS like(likeness string)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('ALTER EDGE like change (likeness double)')
        cls.check_resp_succeeded(resp)

    def test_over_all(self):
        time.sleep(self.delay)
        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES 1:(\'Bob\', 10)')
        self.check_resp_succeeded(resp)

        resp = self.execute(
            'INSERT VERTEX person(name, age) VALUES 2:(\'Lily\', 9)')
        self.check_resp_succeeded(resp)

        resp = self.execute('INSERT EDGE like(likeness) VALUES 1->2:(80.0)')
        self.check_resp_succeeded(resp)

        resp = self.execute('GO FROM 1 OVER * YIELD $$.person.name, '
                                  '$$.person.age')
        self.check_resp_succeeded(resp)

        expect_result = [['Lily', 9]]
        self.check_result(resp.rows, expect_result)

    @classmethod
    def cleanup(cls):
        resp = cls.execute('drop space fixed_over_all_pr1962')
        cls.check_resp_succeeded(resp)
