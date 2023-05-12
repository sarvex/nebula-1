# --coding:utf-8--
#
# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.
import time

from tests.common.nebula_test_suite import NebulaTestSuite

class TestWhere(NebulaTestSuite):
    @classmethod
    def prepare(cls):
        resp = cls.execute(
            'CREATE SPACE IF NOT EXISTS nbaWHERE(partition_num={partition_num}, replica_factor={replica_factor})'.format(
                partition_num=cls.partition_num, replica_factor=cls.replica_factor
            )
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute('USE nbaWHERE')
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG player (name string, age int)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG team (name string)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG INDEX player_index_1 on player(name);")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG INDEX player_index_2 on player(age);")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG INDEX player_index_3 on player(name,age);")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE TAG INDEX team_index_1 on team(name)")
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        resp = cls.execute(
            'INSERT VERTEX player(name, age) VALUES 100:("Tim Duncan", 42)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT VERTEX player(name, age) VALUES 101:("Tony Parker", 36);'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT VERTEX player(name, age) VALUES 102:("LaMarcus Aldridge", 33);'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX player(name, age) VALUES 103:("xxx", 35);')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX player(name, age) VALUES 104:("yyy", 28);')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX player(name, age) VALUES 105:("zzz", 21);')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX player(name, age) VALUES 106:("kkk", 21);')
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT VERTEX player(name, age) VALUES 121:("Useless", 60);'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT VERTEX player(name, age) VALUES 121:("Useless", 20);'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX team(name) VALUES 200:("Warriors");')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX team(name) VALUES 201:("Nuggets")')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX team(name) VALUES 202:("oopp")')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX team(name) VALUES 203:("iiiooo")')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT VERTEX team(name) VALUES 204:("opl")')
        cls.check_resp_succeeded(resp)

        resp = cls.execute('USE nbaWHERE')
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE EDGE like(likeness int)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE EDGE serve(start_year int, end_year int)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE EDGE INDEX serve_index_1 on serve(start_year)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE EDGE INDEX serve_index_2 on serve(end_year)")
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            "CREATE EDGE INDEX serve_index_3 on serve(start_year, end_year)"
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute("CREATE EDGE INDEX like_index_1 on like(likeness)")
        cls.check_resp_succeeded(resp)
        time.sleep(cls.delay)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 100 -> 101:(95)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 101 -> 102:(95)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 102 -> 104:(85)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 102 -> 103:(85)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 105 -> 106:(90)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute('INSERT EDGE like(likeness) VALUES 106 -> 100:(75)')
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 100 -> 200:(1997, 2016)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 101 -> 201:(1999, 2018)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 102 -> 202:(1997, 2016)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 103 -> 203:(1999, 2018)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 105 -> 204:(1997, 2016)'
        )
        cls.check_resp_succeeded(resp)
        resp = cls.execute(
            'INSERT EDGE serve(start_year, end_year) VALUES 121 -> 201:(1999, 2018)'
        )
        cls.check_resp_succeeded(resp)

    def test_where_and(self):
        resp = self.execute('LOOKUP ON serve where serve.start_year > 2000 and serve.end_year < 2020')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON player where player.name == Useless and player.age < 30')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON like where like.likeness > 89 and like.likeness < 100')
        self.check_resp_succeeded(resp)

    def test_where_or(self):
        resp = self.execute('LOOKUP ON like where like.likeness < 39 or like.likeness > 40')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON like where like.likeness < 39 or like.likeness < 40')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON serve where serve.start_year > 2000 or serve.end_year < 2020')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON player where player.name == Useless or player.age < 30')
        self.check_resp_succeeded(resp)
        resp = self.execute('LOOKUP ON like where like.likeness > 89 or like.likeness < 100')
        self.check_resp_succeeded(resp)

    @classmethod
    def cleanup(cls):
        resp = cls.execute('drop space nbaWHERE')
        cls.check_resp_succeeded(resp)
