# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import time
import string
import random

import pytest

from nebula3.common import ttypes

from tests.common.nebula_test_suite import NebulaTestSuite


class TestRangeChecking(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        resp = cls.execute(
            'CREATE SPACE test_range_checking(partition_num={partition_num}, replica_factor={replica_factor}, vid_type=FIXED_STRING(8))'.format(
                partition_num=cls.partition_num, replica_factor=cls.replica_factor
            )
        )
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)

        resp = cls.execute('USE test_range_checking')
        cls.check_resp_succeeded(resp)

        resp = cls.execute('CREATE TAG test(id int)')
        cls.check_resp_succeeded(resp)

    def gen_name(self, length: int) -> str:
        return ''.join(random.choice(string.ascii_letters) for _ in range(length))

    @pytest.mark.parametrize('length', [1, 2048, 4096])
    def test_label_length_valid(self, length):
        query = f'CREATE TAG {self.gen_name(length)}(id int)'
        resp = self.execute(query)
        self.check_resp_succeeded(resp)

    @pytest.mark.parametrize('length', [4097, 4444, 10240])
    def test_label_length_invalid(self, length):
        query = f'CREATE TAG {self.gen_name(length)}(id int)'
        resp = self.execute(query)
        self.check_resp_failed(resp, ttypes.ErrorCode.E_SYNTAX_ERROR)

    @classmethod
    def cleanup(cls):
        resp = cls.execute('DROP SPACE test_range_checking')
        cls.check_resp_succeeded(resp)
