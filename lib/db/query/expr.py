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


class Expr(object):
    """Sql expresion builder"""

    def __init__(self, expression, alias=None):
        #: sql expresion
        self.expression = expression
        #: expresssion filed  name
        self.alias = alias

    def compile(self, db):
        """Building the sql expression

        :param db: the database instance
        """
        sql = self.expression
        if self.alias:
            sql += (' AS ' + db.quote_column(self.alias))
        return sql
