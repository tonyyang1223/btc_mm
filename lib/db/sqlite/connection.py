#!/usr/bin/env python
# Copyright (C) 2014-2015 Thomas Huang
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, version 2 of the License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from db.connection import Connection
import sqlite3

from db.errors import (
    NotInstallDriverError,
)

import time
import logging

LOGGER = logging.getLogger('db.sqlite')


class SQLiteConnection(Connection):

    def initialize(self):
        self._last_used = time.time()
        self._max_idle = self._db_options.pop('max_idle', 10)

    def default_options(self):
        return {          
            'database': 'test.db',
            
        }

    def connect(self):
        self.close()
        self._connect = sqlite3.connect(**self._db_options)
        self._connect.isolation_level = None

    def ensure_connect(self):
        if not self._connect or self._max_idle < (time.time() - self._last_used):
            self.connect()
        self._last_used = time.time()

    def cursor(self,as_dict=False):
        self.ensure_connect()
        if as_dict:
            self._connect.row_factory = self.dict_factory
        return self._connect.cursor()
    def driver(self):
        return 'SQLite'
    @staticmethod
    def dict_factory(cursor,row):
        d={}
        for idx,col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def real_ctype(self, as_dict=False):
        if as_dict:
            return DictCursor
        return Cursor
    @staticmethod
    def sql_fix(sql,args):
        if args is None:
            args ={}
        return sql,args
    def autocommit(self, enable=True):
        if enable:
            self._connect.isolation_level = None
        else:
            self._connect.isolation_level = "DEFERRED"
        
