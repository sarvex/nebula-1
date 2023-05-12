# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

import pytest
import time

from tests.common.nebula_test_suite import NebulaTestSuite


class TestAdmin(NebulaTestSuite):
    '''
    @brief Testing suite about administration function
    '''

    @classmethod
    def prepare(cls):
        pass

    @classmethod
    def cleanup(cls):
        pass

    @pytest.mark.skip(reason="The change of minloglevel will influence case in test_configs.py")
    def test_config(self):
        '''
        @brief Testing about configuration query
        '''
        # List
        resp = self.client.execute('SHOW CONFIGS meta')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS graph')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS storage')
        self.check_resp_succeeded(resp)
        resp = self.client.execute('SHOW CONFIGS')
        self.check_resp_succeeded(resp)

        # Backup
        resp = self.client.execute('GET CONFIGS graph:minloglevel')
        self.check_resp_succeeded(resp)
        graph_minloglevel = resp.row_values(0)[4].as_int()
        resp = self.client.execute('GET CONFIGS storage:minloglevel')
        self.check_resp_succeeded(resp)
        storage_minloglevel = resp.row_values(0)[4].as_int()

        # Set
        minloglevel = 3
        resp = self.client.execute(f'UPDATE CONFIGS meta:minloglevel={minloglevel}')
        self.check_resp_failed(resp)
        resp = self.client.execute(f'UPDATE CONFIGS graph:minloglevel={minloglevel}')
        self.check_resp_succeeded(resp)
        resp = self.client.execute(f'UPDATE CONFIGS storage:minloglevel={minloglevel}')
        self.check_resp_succeeded(resp)

        # get
        resp = self.client.execute('GET CONFIGS meta:minloglevel')
        self.check_resp_failed(resp)
        result = [['GRAPH', 'minloglevel', 'int', 'MUTABLE', minloglevel]]
        resp = self.client.execute('GET CONFIGS graph:minloglevel')
        self.check_resp_succeeded(resp)
        self.check_result(resp, result)
        result = [['STORAGE', 'minloglevel', 'int', 'MUTABLE', minloglevel]]
        resp = self.client.execute('GET CONFIGS storage:minloglevel')
        self.check_resp_succeeded(resp)
        self.check_result(resp, result)

        # rollback
        resp = self.client.execute(
            f'UPDATE CONFIGS graph:minloglevel={int(graph_minloglevel)}'
        )
        print(f'UPDATE CONFIGS graph:minloglevel={graph_minloglevel}')
        self.check_resp_succeeded(resp)
        resp = self.client.execute(
            f'UPDATE CONFIGS storage:minloglevel={int(storage_minloglevel)}'
        )
        self.check_resp_succeeded(resp)
        time.sleep(self.delay)
