"""
Database API
(part of web.py)
"""
from __future__ import print_function
from .utils import threadeddict, storage, iters, iterbetter
import time, re
from AccuradSite import settings
import base64
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

from .utils import string_types, numeric_types, iteritems
import tools

try:
    from urllib import parse as urlparse
    from urllib.parse import unquote
except ImportError:
    import urlparse
    from urllib import unquote

try:
    import ast
except ImportError:
    ast = None

import sys
debug = sys.stderr
config = storage()

__all__ = [
  "UnknownParamstyle", "UnknownDB", "TransactionError", 
  "sqllist", "sqlors", "reparam", "sqlquote",
  "SQLQuery", "SQLParam", "sqlparam",
  "SQLLiteral", "sqlliteral",
  "database", 'DB',
]

TOKEN = '[ \\f\\t]*(\\\\\\r?\\n[ \\f\\t]*)*(#[^\\r\\n]*)?(((\\d+[jJ]|((\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?|\\d+[eE][-+]?\\d+)[jJ])|((\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?|\\d+[eE][-+]?\\d+)|(0[xX][\\da-fA-F]+[lL]?|0[bB][01]+[lL]?|(0[oO][0-7]+)|(0[0-7]*)[lL]?|[1-9]\\d*[lL]?))|((\\*\\*=?|>>=?|<<=?|<>|!=|//=?|[+\\-*/%&|^=<>]=?|~)|[][(){}]|(\\r?\\n|[:;.,`@]))|([uUbB]?[rR]?\'[^\\n\'\\\\]*(?:\\\\.[^\\n\'\\\\]*)*\'|[uUbB]?[rR]?"[^\\n"\\\\]*(?:\\\\.[^\\n"\\\\]*)*")|[a-zA-Z_]\\w*)'

tokenprog = re.compile(TOKEN)


class UnknownDB(Exception):
    """raised for unsupported dbms"""
    pass


class _ItplError(ValueError): 
    def __init__(self, text, pos):
        ValueError.__init__(self)
        self.text = text
        self.pos = pos

    def __str__(self):
        return "unfinished expression in %s at char %d" % (
            repr(self.text), self.pos)


class TransactionError(Exception): pass


class UnknownParamstyle(Exception): 
    """
    raised for unsupported db paramstyles

    (currently supported: qmark, numeric, format, pyformat)
    """
    pass


class SQLParam(object):
    """
    Parameter in SQLQuery.
    
        >>> q = SQLQuery(["SELECT * FROM test WHERE name=", SQLParam("joe")])
        >>> q
        <sql: "SELECT * FROM test WHERE name='joe'">
        >>> q.query()
        'SELECT * FROM test WHERE name=%s'
        >>> q.values()
        ['joe']
    """
    __slots__ = ["value"]

    def __init__(self, value):
        self.value = value
        
    def get_marker(self, paramstyle='pyformat'):
        if paramstyle == 'qmark':
            return '?'
        elif paramstyle == 'numeric':
            return ':1'
        elif paramstyle is None or paramstyle in ['format', 'pyformat']:
            return '%s'
        raise UnknownParamstyle(paramstyle)
        
    def sqlquery(self): 
        return SQLQuery([self])
        
    def __add__(self, other):
        return self.sqlquery() + other
        
    def __radd__(self, other):
        return other + self.sqlquery() 
            
    def __str__(self): 
        return str(self.value)

    def __eq__(self, other):
        return isinstance(other, SQLParam) and other.value == self.value
    
    def __repr__(self):
        return '<param: %s>' % repr(self.value)


sqlparam =  SQLParam


class SQLQuery(object):
    """
    You can pass this sort of thing as a clause in any db function.
    Otherwise, you can pass a dictionary to the keyword argument `vars`
    and the function will call reparam for you.

    Internally, consists of `items`, which is a list of strings and
    SQLParams, which get concatenated to produce the actual query.
    """
    __slots__ = ["items"]

    # tested in sqlquote's docstring
    def __init__(self, items=None):
        r"""Creates a new SQLQuery.
        
            >>> SQLQuery("x")
            <sql: 'x'>
            >>> q = SQLQuery(['SELECT * FROM ', 'test', ' WHERE x=', SQLParam(1)])
            >>> q
            <sql: 'SELECT * FROM test WHERE x=1'>
            >>> q.query(), q.values()
            ('SELECT * FROM test WHERE x=%s', [1])
            >>> SQLQuery(SQLParam(1))
            <sql: '1'>
        """
        if items is None:
            self.items = []
        elif isinstance(items, list):
            self.items = items
        elif isinstance(items, SQLParam):
            self.items = [items]
        elif isinstance(items, SQLQuery):
            self.items = list(items.items)
        else:
            self.items = [items]
            
        # Take care of SQLLiterals
        for i, item in enumerate(self.items):
            if isinstance(item, SQLParam) and isinstance(item.value, SQLLiteral):
                self.items[i] = item.value.v

    def append(self, value):
        self.items.append(value)

    def __add__(self, other):
        if isinstance(other, string_types):
            items = [other]
        elif isinstance(other, SQLQuery):
            items = other.items
        else:
            return NotImplemented
        return SQLQuery(self.items + items)

    def __radd__(self, other):
        if isinstance(other, string_types):
            items = [other]
        elif isinstance(other, SQLQuery):
            items = other.items
        else:
            return NotImplemented
        return SQLQuery(items + self.items)

    def __iadd__(self, other):
        if isinstance(other, (string_types, SQLParam)):
            self.items.append(other)
        elif isinstance(other, SQLQuery):
            self.items.extend(other.items)
        else:
            return NotImplemented
        return self

    def __len__(self):
        return len(self.query())

    def __eq__(self, other):
        return isinstance(other, SQLQuery) and other.items == self.items
        
    def query(self, paramstyle=None):
        """
        Returns the query part of the sql query.
            >>> q = SQLQuery(["SELECT * FROM test WHERE name=", SQLParam('joe')])
            >>> q.query()
            'SELECT * FROM test WHERE name=%s'
            >>> q.query(paramstyle='qmark')
            'SELECT * FROM test WHERE name=?'
        """
        s = []
        for x in self.items:
            if isinstance(x, SQLParam):
                x = x.get_marker(paramstyle)
                s.append(str(x))
            else:
                x = str(x)
                # automatically escape % characters in the query
                # For backward compatability, ignore escaping when the query looks already escaped
                if paramstyle in ['format', 'pyformat']:
                    if '%' in x and '%%' not in x:
                        x = x.replace('%', '%%')
                s.append(x)
        return "".join(s)
    
    def values(self):
        """
        Returns the values of the parameters used in the sql query.
            >>> q = SQLQuery(["SELECT * FROM test WHERE name=", SQLParam('joe')])
            >>> q.values()
            ['joe']
        """
        return [i.value for i in self.items if isinstance(i, SQLParam)]
        
    def join(items, sep=' ', prefix=None, suffix=None, target=None):
        """
        Joins multiple queries.
        
        >>> SQLQuery.join(['a', 'b'], ', ')
        <sql: 'a, b'>

        Optinally, prefix and suffix arguments can be provided.

        >>> SQLQuery.join(['a', 'b'], ', ', prefix='(', suffix=')')
        <sql: '(a, b)'>

        If target argument is provided, the items are appended to target instead of creating a new SQLQuery.
        """
        if target is None:
            target = SQLQuery()

        target_items = target.items

        if prefix:
            target_items.append(prefix)

        for i, item in enumerate(items):
            if i != 0 and sep != "":
                target_items.append(sep)
            if isinstance(item, SQLQuery):
                target_items.extend(item.items)
            elif item == "": # joins with empty strings
                continue
            else:
                target_items.append(item)

        if suffix:
            target_items.append(suffix)
        return target
    
    join = staticmethod(join)


class SQLLiteral: 
    """
    Protects a string from `sqlquote`.

        >>> sqlquote('NOW()')
        <sql: "'NOW()'">
        >>> sqlquote(SQLLiteral('NOW()'))
        <sql: 'NOW()'>
    """
    def __init__(self, v): 
        self.v = v

    def __repr__(self): 
        return "<literal: %r>" % self.v


sqlliteral = SQLLiteral


def _sqllist(values):
    """
        >>> _sqllist([1, 2, 3])
        <sql: '(1, 2, 3)'>
    """
    items = []
    items.append('(')
    for i, v in enumerate(values):
        if i != 0:
            items.append(', ')
        items.append(sqlparam(v))
    items.append(')')
    return SQLQuery(items)


def reparam(string_, dictionary): 
    """
    Takes a string and a dictionary and interpolates the string
    using values from the dictionary. Returns an `SQLQuery` for the result.

        >>> reparam("s = $s", dict(s=True))
        <sql: "s = 't'">
        >>> reparam("s IN $s", dict(s=[1, 2]))
        <sql: 's IN (1, 2)'>
    """
    return SafeEval().safeeval(string_, dictionary)

    dictionary = dictionary.copy() # eval mucks with it
    # disable builtins to avoid risk for remote code exection.
    dictionary['__builtins__'] = object()
    result = []
    for live, chunk in _interpolate(string_):
        if live:
            v = eval(chunk, dictionary)
            result.append(sqlquote(v))
        else: 
            result.append(chunk)
    return SQLQuery.join(result, '')


def sqllist(lst): 
    """
    Converts the arguments for use in something like a WHERE clause.
    
        >>> sqllist(['a', 'b'])
        'a, b'
        >>> sqllist('a')
        'a'
    """
    if isinstance(lst, string_types):
        return lst
    else:
        return ', '.join(lst)


def sqlors(left, lst):
    """
    `left is a SQL clause like `tablename.arg = ` 
    and `lst` is a list of values. Returns a reparam-style
    pair featuring the SQL that ORs together the clause
    for each item in the lst.

        >>> sqlors('foo = ', [])
        <sql: '1=2'>
        >>> sqlors('foo = ', [1])
        <sql: 'foo = 1'>
        >>> sqlors('foo = ', 1)
        <sql: 'foo = 1'>
        >>> sqlors('foo = ', [1,2,3])
        <sql: '(foo = 1 OR foo = 2 OR foo = 3 OR 1=2)'>
    """
    if isinstance(lst, iters):
        lst = list(lst)
        ln = len(lst)
        if ln == 0:
            return SQLQuery("1=2")
        if ln == 1:
            lst = lst[0]

    if isinstance(lst, iters):
        return SQLQuery(['('] + 
          sum([[left, sqlparam(x), ' OR '] for x in lst], []) +
          ['1=2)']
        )
    else:
        return left + sqlparam(lst)


def sqlwhere(data, grouping=' AND '):
    """
    Converts a two-tuple (key, value) iterable `data` to an SQL WHERE clause `SQLQuery`.
    
        >>> sqlwhere((('cust_id', 2), ('order_id',3)))
        <sql: 'cust_id = 2 AND order_id = 3'>
        >>> sqlwhere((('order_id', 3), ('cust_id', 2)), grouping=', ')
        <sql: 'order_id = 3, cust_id = 2'>
        >>> sqlwhere((('a', 'a'), ('b', 'b'))).query()
        'a = %s AND b = %s'
    """

    return SQLQuery.join([k + ' = ' + sqlparam(v) for k, v in data], grouping)


def sqlquote(a): 
    """
    Ensures `a` is quoted properly for use in a SQL query.

        >>> 'WHERE x = ' + sqlquote(True) + ' AND y = ' + sqlquote(3)
        <sql: "WHERE x = 't' AND y = 3">
        >>> 'WHERE x = ' + sqlquote(True) + ' AND y IN ' + sqlquote([2, 3])
        <sql: "WHERE x = 't' AND y IN (2, 3)">
    """
    if isinstance(a, list):
        return _sqllist(a)
    else:
        return sqlparam(a).sqlquery()


class Transaction:
    """Database transaction."""
    def __init__(self, ctx):
        self.ctx = ctx
        self.transaction_count = transaction_count = len(ctx.transactions)

        class transaction_engine:
            """Transaction Engine used in top level transactions."""
            def do_transact(self):
                ctx.commit(unload=False)

            def do_commit(self):
                ctx.commit()

            def do_rollback(self):
                ctx.rollback()

        class subtransaction_engine:
            """Transaction Engine used in sub transactions."""
            def query(self, q):
                db_cursor = ctx.db.cursor()
                ctx.db_execute(db_cursor, SQLQuery(q % transaction_count))

            def do_transact(self):
                self.query('SAVEPOINT webpy_sp_%s')

            def do_commit(self):
                self.query('RELEASE SAVEPOINT webpy_sp_%s')

            def do_rollback(self):
                self.query('ROLLBACK TO SAVEPOINT webpy_sp_%s')

        class dummy_engine:
            """Transaction Engine used instead of subtransaction_engine 
            when sub transactions are not supported."""
            do_transact = do_commit = do_rollback = lambda self: None

        if self.transaction_count:
            # nested transactions are not supported in some databases
            if self.ctx.get('ignore_nested_transactions'):
                self.engine = dummy_engine()
            else:
                self.engine = subtransaction_engine()
        else:
            self.engine = transaction_engine()

        self.engine.do_transact()
        self.ctx.transactions.append(self)

    def __enter__(self):
        return self

    def __exit__(self, exctype, excvalue, traceback):
        if exctype is not None:
            self.rollback()
        else:
            self.commit()

    def commit(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.do_commit()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]

    def rollback(self):
        if len(self.ctx.transactions) > self.transaction_count:
            self.engine.do_rollback()
            self.ctx.transactions = self.ctx.transactions[:self.transaction_count]


class DB: 
    """Database"""
    def __init__(self, db_module, keywords, db_name):
        """Creates a database.
        """
        # some DB implementaions take optional paramater `driver` to use a specific driver modue
        # but it should not be passed to connect
        keywords.pop('driver', None)

        self.db_module = db_module
        self.keywords = keywords
        self.db_name = db_name

        settings.LOCK.acquire()
        print("settings ctxs(%s): " % os.getpid(), settings.CTXS)
        self.dbmark = self.caculateDBMark(keywords)
        if settings.CTXS.keys().__contains__(self.dbmark) and settings.CTXS[self.dbmark].get('db'):
            self._ctx = settings.CTXS[self.dbmark]
        else:
            self._ctx = threadeddict()
            settings.CTXS[self.dbmark] = self._ctx
        settings.LOCK.release()

        # flag to enable/disable printing queries
        self.printing = config.get('debug_sql', config.get('debug', False))
        self.supports_multiple_insert = False
        
        try:
            import DBUtils
            # enable pooling if DBUtils module is available.
            self.has_pooling = True
        except ImportError:
            self.has_pooling = False
            
        # Pooling can be disabled by passing pooling=False in the keywords.
        self.has_pooling = self.keywords.pop('pooling', True) and self.has_pooling

    def caculateDBMark(self, keywords):
        mark = "accu_"
        if keywords.keys().__contains__('host'):
            mark = mark + keywords['host']
        if keywords.keys().__contains__('port'):
            mark = mark + str(keywords['port'])
        if keywords.keys().__contains__('user'):
            mark = mark + str(keywords['user'])
        if keywords.keys().__contains__('password'):
            mark = mark + str(keywords['password'])
        if keywords.keys().__contains__('dsn'):
            mark = mark + str(base64.b64encode(keywords['dsn'].encode('utf-8')), 'utf-8')

        return str(base64.b64encode(mark.encode('utf-8')), 'utf-8')
            
    def _getctx(self):
        if not self._ctx.get('db'):
            self._load_context()
        return self._ctx
    ctx = property(_getctx)

    def _load_context(self):
        self._ctx.dbq_count = 0
        self._ctx.transactions = []  # stack of transactions

        if self.has_pooling:
            print('*-*' * 25, 'create new db pooling', '*-*' * 25)
            self._ctx.db = self._connect_with_pooling(self.keywords)
        else:
            print('-*-' * 25, 'create new db connect:(%s)' % os.getpid(), '-*-' * 25)
            self._ctx.db = self._connect(self.keywords)
        self._ctx.db_execute = self._db_execute
        
        if not hasattr(self._ctx.db, 'commit'):
            self._ctx.db.commit = lambda: None

        if not hasattr(self._ctx.db, 'rollback'):
            self._ctx.db.rollback = lambda: None
            
        def commit(unload=False):
            # do db commit and release the connection if pooling is enabled.            
            self._ctx.db.commit()
            if unload and self.has_pooling:
                self._unload_context()
                
        def rollback():
            # do db rollback and release the connection if pooling is enabled.
            self._ctx.db.rollback()
            if self.has_pooling:
                self._unload_context()

        self._ctx.commit = commit
        self._ctx.rollback = rollback
            
    def _unload_context(self):
        del self._ctx.db
            
    def _connect(self, keywords):
        return self.db_module.connect(**keywords)
        
    def _connect_with_pooling(self, keywords):
        def get_pooled_db():
            from DBUtils import PooledDB

            # In DBUtils 0.9.3, `dbapi` argument is renamed as `creator`
            # see Bug#122112
            
            if PooledDB.__version__.split('.') < '0.9.3'.split('.'):
                return PooledDB.PooledDB(dbapi=self.db_module, **keywords)
            else:
                return PooledDB.PooledDB(creator=self.db_module, **keywords)
        
        if getattr(self, '_pooleddb', None) is None:
            self._pooleddb = get_pooled_db()
        
        return self._pooleddb.connection()
        
    def _db_cursor(self):
        return self.ctx.db.cursor()

    def _param_marker(self):
        """Returns parameter marker based on paramstyle attribute if this database."""
        style = getattr(self, 'paramstyle', 'pyformat')

        if style == 'qmark':
            return '?'
        elif style == 'numeric':
            return ':1'
        elif style in ['format', 'pyformat']:
            return '%s'
        raise UnknownParamstyle(style)

    def _db_execute(self, cur, sql_query): 
        """executes an sql query"""
        self.ctx.dbq_count += 1
        
        try:
            a = time.time()
            query, params = self._process_query(sql_query)
            out = cur.execute(query, params)
            b = time.time()
        except:
            if self.printing:
                print('ERR:', str(query), file=debug)
            try:
                if self.ctx.transactions:
                    self.ctx.transactions[-1].rollback()
                else:
                    self.ctx.rollback()
            except:
                try:
                    self.closedb()
                except:
                    raise
                raise
            try:
                self.closedb()
            except:
                raise
            raise

        if self.printing:
            print('%s (%s): %s' % (round(b-a, 2), self.ctx.dbq_count, str(query)), file=debug)
        return out

    def _process_query(self, sql_query):
        """Takes the SQLQuery object and returns query string and parameters.
        """
        paramstyle = getattr(self, 'paramstyle', 'pyformat')
        query = sql_query.query(paramstyle)
        params = sql_query.values()
        return query, params
    
    def _where(self, where, vars): 
        if isinstance(where, numeric_types):
            where = "id = " + sqlparam(where)
        #@@@ for backward-compatibility
        elif isinstance(where, (list, tuple)) and len(where) == 2:
            where = SQLQuery(where[0], where[1])
        elif isinstance(where, dict):
            where = self._where_dict(where)
        elif isinstance(where, SQLQuery):
            pass
        else:
            where = reparam(where, vars)        
        return where

    def _where_dict(self, where):
        where_clauses = []
        
        for k, v in sorted(iteritems(where), key= lambda t:t[0]):
            where_clauses.append(k + ' = ' + sqlquote(v))
        if where_clauses:
            return SQLQuery.join(where_clauses, " AND ")
        else:
            return None
    
    def query(self, sql_query, vars=None, processed=False, _test=False): 
        """
        Execute SQL query `sql_query` using dictionary `vars` to interpolate it.
        If `processed=True`, `vars` is a `reparam`-style list to use 
        instead of interpolating.
        
            >>> db = DB(None, {})
            >>> db.query("SELECT * FROM foo", _test=True)
            <sql: 'SELECT * FROM foo'>
            >>> db.query("SELECT * FROM foo WHERE x = $x", vars=dict(x='f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
            >>> db.query("SELECT * FROM foo WHERE x = " + sqlquote('f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
        """
        if vars is None: vars = {}
        
        if not processed and not isinstance(sql_query, SQLQuery):
            sql_query = reparam(sql_query, vars)
        
        if _test: return sql_query

        db_cursor = self._db_cursor()
        try:
            self._db_execute(db_cursor, sql_query)
        except:
            raise

        if db_cursor.description:
            names = [x[0] for x in db_cursor.description]

            def iterwrapper(results):
                if results is None:
                    return
                for row in results:
                    yield storage(dict(zip(names, row)))
            out = iterbetter(iterwrapper(db_cursor.fetchall()))
            out.__len__ = lambda: int(db_cursor.rowcount)
            out.list = lambda: [storage(dict(zip(names, x))) \
                               for x in db_cursor.fetchall()]
        else:
            out = db_cursor.rowcount

        if not self.ctx.transactions:
            self.ctx.commit()
        if isinstance(out, int) and out < 0:
            self.closedb()
        return out
    
    def select(self, tables, vars=None, what='*', where=None, order=None, group=None, 
               limit=None, offset=None, _test=False): 
        """
        Selects `what` from `tables` with clauses `where`, `order`, 
        `group`, `limit`, and `offset`. Uses vars to interpolate. 
        Otherwise, each clause can be a SQLQuery.
        
            >>> db = DB(None, {})
            >>> db.select('foo', _test=True)
            <sql: 'SELECT * FROM foo'>
            >>> db.select(['foo', 'bar'], where="foo.bar_id = bar.id", limit=5, _test=True)
            <sql: 'SELECT * FROM foo, bar WHERE foo.bar_id = bar.id LIMIT 5'>
            >>> db.select('foo', where={'id': 5}, _test=True)
            <sql: 'SELECT * FROM foo WHERE id = 5'>
        """
        if vars is None: vars = {}
        sql_clauses = self.sql_clauses(what, tables, where, group, order, limit, offset)
        clauses = [self.gen_clause(sql, val, vars) for sql, val in sql_clauses if val is not None]
        qout = SQLQuery.join(clauses)
        if _test: return qout
        try:
            out = self.query(qout, processed=True)
        except:
            raise
        return out
    
    def where(self, table, what='*', order=None, group=None, limit=None, 
              offset=None, _test=False, **kwargs):
        """
        Selects from `table` where keys are equal to values in `kwargs`.
        
            >>> db = DB(None, {})
            >>> db.where('foo', bar_id=3, _test=True)
            <sql: 'SELECT * FROM foo WHERE bar_id = 3'>
            >>> db.where('foo', source=2, crust='dewey', _test=True)
            <sql: "SELECT * FROM foo WHERE crust = 'dewey' AND source = 2">
            >>> db.where('foo', _test=True)
            <sql: 'SELECT * FROM foo'>
        """
        where = self._where_dict(kwargs)            
        return self.select(table, what=what, order=order, 
               group=group, limit=limit, offset=offset, _test=_test, 
               where=where)
    
    def sql_clauses(self, what, tables, where, group, order, limit, offset): 
        return (
            ('SELECT', what),
            ('FROM', sqllist(tables)),
            ('WHERE', where),
            ('GROUP BY', group),
            ('ORDER BY', order),
            # The limit and offset could be the values provided by
            # the end-user and are potentially unsafe.
            # Using them as parameters to avoid any risk.
            ('LIMIT', limit and SQLParam(limit).sqlquery()),
            ('OFFSET', offset and SQLParam(offset).sqlquery()))
    
    def gen_clause(self, sql, val, vars): 
        if isinstance(val, numeric_types):
            if sql == 'WHERE':
                nout = 'id = ' + sqlquote(val)
            else:
                nout = SQLQuery(val)
        #@@@
        elif isinstance(val, (list, tuple)) and len(val) == 2:
            nout = SQLQuery(val[0], val[1]) # backwards-compatibility
        elif sql == 'WHERE' and isinstance(val, dict):
            nout = self._where_dict(val)
        elif isinstance(val, SQLQuery):
            nout = val
        else:
            nout = reparam(val, vars)

        def xjoin(a, b):
            if a and b: return a + ' ' + b
            else: return a or b

        return xjoin(sql, nout)

    def insert(self, tablename, seqname=None, _test=False, **values): 
        """
        Inserts `values` into `tablename`. Returns current sequence ID.
        Set `seqname` to the ID if it's not the default, or to `False`
        if there isn't one.
        
            >>> db = DB(None, {})
            >>> q = db.insert('foo', name='bob', age=2, created=SQLLiteral('NOW()'), _test=True)
            >>> q
            <sql: "INSERT INTO foo (age, created, name) VALUES (2, NOW(), 'bob')">
            >>> q.query()
            'INSERT INTO foo (age, created, name) VALUES (%s, NOW(), %s)'
            >>> q.values()
            [2, 'bob']
        """
        def q(x): return "(" + x + ")"
        
        if values:
            #needed for Py3 compatibility with the above doctests
            sorted_values = sorted(values.items(), key=lambda t: t[0]) 

            _keys = SQLQuery.join(map(lambda t: t[0], sorted_values), ', ')
            _values = SQLQuery.join([sqlparam(v) for v in map(lambda t: t[1], sorted_values)], ', ')
            sql_query = "INSERT INTO %s " % tablename + q(_keys) + ' VALUES ' + q(_values)
        else:
            sql_query = SQLQuery(self._get_insert_default_values_query(tablename))

        if _test: return sql_query
        
        db_cursor = self._db_cursor()
        if seqname is not False: 
            sql_query = self._process_insert_query(sql_query, tablename, seqname)


        if isinstance(sql_query, tuple):
            # for some databases, a separate query has to be made to find 
            # the id of the inserted row.
            q1, q2 = sql_query
            try:
                self._db_execute(db_cursor, q1)
                self._db_execute(db_cursor, q2)
            except:
                raise
        else:
            try:
                self._db_execute(db_cursor, sql_query)
            except:
                raise

        try: 
            out = db_cursor.fetchone()[0]
        except Exception: 
            out = None

        if not self.ctx.transactions: 
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()

        return out
        
    def _get_insert_default_values_query(self, table):
        return "INSERT INTO %s DEFAULT VALUES" % table

    def multiple_insert(self, tablename, values, seqname=None, _test=False):
        """
        Inserts multiple rows into `tablename`. The `values` must be a list of dictioanries, 
        one for each row to be inserted, each with the same set of keys.
        Returns the list of ids of the inserted rows.        
        Set `seqname` to the ID if it's not the default, or to `False`
        if there isn't one.
        
            >>> db = DB(None, {})
            >>> db.supports_multiple_insert = True
            >>> values = [{"name": "foo", "email": "foo@example.com"}, {"name": "bar", "email": "bar@example.com"}]
            >>> db.multiple_insert('person', values=values, _test=True)
            <sql: "INSERT INTO person (email, name) VALUES ('foo@example.com', 'foo'), ('bar@example.com', 'bar')">
        """        
        if not values:
            return []
            
        if not self.supports_multiple_insert:
            try:
                out = [self.insert(tablename, seqname=seqname, _test=_test, **v) for v in values]
            except:
                raise
            if seqname is False:
                return None
            else:
                return out
                
        keys = values[0].keys()
        #@@ make sure all keys are valid

        for v in values:
            if v.keys() != keys:
                raise ValueError('Not all rows have the same keys')

        keys = sorted(keys) #enforce query order for the above doctest compatibility with Py3

        if self.db_name == "oracle":
            sql_query = SQLQuery('INSERT ALL INTO %s (%s) VALUES ' % (tablename, ', '.join(keys)))
        else:
            sql_query = SQLQuery('INSERT INTO %s (%s) VALUES ' % (tablename, ', '.join(keys)))

        for i, row in enumerate(values):
            if i != 0:
                if self.db_name == "oracle":
                    sql_query.append(" INTO %s VALUES" % tablename)
                else:
                    sql_query.append(", ")
            SQLQuery.join([SQLParam(row[k]) for k in keys], sep=", ", target=sql_query, prefix="(", suffix=")")

        if self.db_name == "oracle":
            sql_query = sql_query + " select 1 from dual "
        if _test: return sql_query

        db_cursor = self._db_cursor()
        if seqname is not False: 
            sql_query = self._process_insert_query(sql_query, tablename, seqname)

        if isinstance(sql_query, tuple):
            # for some databases, a separate query has to be made to find 
            # the id of the inserted row.
            q1, q2 = sql_query
            try:
                self._db_execute(db_cursor, q1)
                self._db_execute(db_cursor, q2)
            except:
                raise
        else:
            try:
                self._db_execute(db_cursor, sql_query)
            except:
                raise

        try: 
            out = db_cursor.fetchone()[0]
            out = range(out-len(values)+1, out+1)        
        except Exception: 
            out = None

        if not self.ctx.transactions: 
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()
        return out

    def update(self, tables, where, vars=None, _test=False, **values): 
        """
        Update `tables` with clause `where` (interpolated using `vars`)
        and setting `values`.

            >>> db = DB(None, {})
            >>> name = 'Joseph'
            >>> q = db.update('foo', where='name = $name', name='bob', age=2,
            ...     created=SQLLiteral('NOW()'), vars=locals(), _test=True)
            >>> q
            <sql: "UPDATE foo SET age = 2, created = NOW(), name = 'bob' WHERE name = 'Joseph'">
            >>> q.query()
            'UPDATE foo SET age = %s, created = NOW(), name = %s WHERE name = %s'
            >>> q.values()
            [2, 'bob', 'Joseph']
        """
        if vars is None: vars = {}
        where = self._where(where, vars)

        values = sorted(values.items(), key=lambda t: t[0]) 

        query = (
          "UPDATE " + sqllist(tables) + 
          " SET " + sqlwhere(values, ', ') + 
          " WHERE " + where)

        if _test: return query
        
        db_cursor = self._db_cursor()
        try:
            self._db_execute(db_cursor, query)
        except:
            raise
        if not self.ctx.transactions: 
            self.ctx.commit()
        out = db_cursor.rowcount
        if out < 0:
            self.closedb()
        return out
    
    def delete(self, table, where, using=None, vars=None, _test=False): 
        """
        Deletes from `table` with clauses `where` and `using`.

            >>> db = DB(None, {})
            >>> name = 'Joe'
            >>> db.delete('foo', where='name = $name', vars=locals(), _test=True)
            <sql: "DELETE FROM foo WHERE name = 'Joe'">
        """
        if vars is None: vars = {}
        where = self._where(where, vars)

        q = 'DELETE FROM ' + table
        if using: q += ' USING ' + sqllist(using)
        if where: q += ' WHERE ' + where

        if _test: return q

        db_cursor = self._db_cursor()
        try:
            self._db_execute(db_cursor, q)
        except:
            raise
        if not self.ctx.transactions: 
            self.ctx.commit()

        out = db_cursor.rowcount
        if out < 0:
            self.closedb()
        return out

    def _process_insert_query(self, query, tablename, seqname):
        return query

    def transaction(self): 
        """Start a transaction."""
        return Transaction(self.ctx)

    def closedb(self):
        try:
            settings.CTXS.pop(self.dbmark)
            self._ctx.db.cursor().connection.close()
        except:
            raise

    def closecursor(self):
        self._db_cursor().close()

    def exec(self, sql):
        out = SQLQuery([sql])
        return self.query(sql_query=out, processed=True)

    def active(self):
        try:
            if self.db_name == "mssql":
                self.ctx.db.ping()
            return True
        except:
            return False


class PostgresDB(DB): 
    """Postgres driver."""
    def __init__(self, **keywords):
        if 'pw' in keywords:
            keywords['password'] = keywords.pop('pw')
            
        db_module = import_driver(["psycopg2", "psycopg", "pgdb"], preferred=keywords.pop('driver', None))
        if db_module.__name__ == "psycopg2":
            import psycopg2.extensions
            psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
        if db_module.__name__ == "pgdb" and 'port' in keywords:
            keywords["host"] += ":" + str(keywords.pop('port'))

        # if db is not provided postgres driver will take it from PGDATABASE environment variable
        if 'db' in keywords:
            keywords['database'] = keywords.pop('db')
        
        self.dbname = "postgres"
        self.paramstyle = db_module.paramstyle
        DB.__init__(self, db_module, keywords)
        self.supports_multiple_insert = True
        self._sequences = None
        
    def _process_insert_query(self, query, tablename, seqname):
        if seqname is None:
            # when seqname is not provided guess the seqname and make sure it exists
            seqname = tablename + "_id_seq"
            if seqname not in self._get_all_sequences():
                seqname = None
        
        if seqname:
            query += "; SELECT currval('%s')" % seqname
            
        return query
    
    def _get_all_sequences(self):
        """Query postgres to find names of all sequences used in this database."""
        if self._sequences is None:
            q = "SELECT c.relname FROM pg_class c WHERE c.relkind = 'S'"
            self._sequences = set([c.relname for c in self.query(q)])
        return self._sequences

    def _connect(self, keywords):
        conn = DB._connect(self, keywords)
        try:
            conn.set_client_encoding('UTF8')
        except AttributeError:
            # fallback for pgdb driver
            conn.cursor().execute("set client_encoding to 'UTF-8'")
        return conn
        
    def _connect_with_pooling(self, keywords):
        conn = DB._connect_with_pooling(self, keywords)
        conn._con._con.set_client_encoding('UTF8')
        return conn


class MySQLDB(DB): 
    def __init__(self, **keywords):

        db = import_driver(["MySQLdb", "pymysql","mysql.connector"], preferred=keywords.pop('driver', None))
        if db.__name__ == "MySQLdb":
            if 'pw' in keywords:
                keywords['passwd'] = keywords['pw']
                del keywords['pw']
        if db.__name__ == "pymysql":
            if 'pw' in keywords:
                keywords['password'] = keywords['pw']
                del keywords['pw']
        if db.__name__ == "mysql.connector":
            if 'pw' in keywords:
                keywords['password'] = keywords['pw']
                del keywords['pw']

        if 'charset' not in keywords:
            keywords['charset'] = 'utf8'
        elif keywords['charset'] is None:
            del keywords['charset']

        self.paramstyle = db.paramstyle = 'pyformat' # it's both, like psycopg
        self.dbname = "mysql"
        DB.__init__(self, db, keywords, self.dbname)
        self.supports_multiple_insert = True

    def callproc(self, name, parmin=[], parmout=[], has_return=False):
        len_parmin = len(parmin)
        len_parmout = len(parmout)

        pp = parmin

        contain = False
        if parmout.__contains__(tools.CURSOR):
            contain = True
        else:
            contain = False

        # 解析tools参数到sql参数
        for i in range(len(parmout)):
            if parmout[i] == tools.CURSOR:
                continue
            pp.append(self.get_parm(parmout[i]))

        cursor = self._db_cursor()

        try:
            results = []
            cursor.callproc(name, tuple(pp))
            if contain:
                ret = cursor.fetchall()
                for cell in ret:
                    results.append(cell)
            if (contain and len_parmout > 1) or (not contain and len_parmout > 0):
                # length = len_parmout
                cur_count = 0
                selectsql = 'select '
                for i in range(len_parmout):
                    if parmout[i] == tools.CURSOR:
                        cur_count = cur_count + 1
                        continue
                    selectsql = selectsql + '@_%s_%d, ' % (name, len_parmin + i - cur_count)
                selectsql = selectsql.rstrip(', ') + ';'

                cursor.execute(selectsql)
                ret = cursor.fetchone()
                cur_count = 0
                for i in range(len(parmout)):
                    if parmout[i] == tools.CURSOR:
                        cur_count = cur_count + 1
                        continue
                    parmout[i] = ret[i - cur_count]

            if contain:
                for i in range(len_parmout):
                    if parmout[i] == tools.CURSOR:
                        parmout[i] = results

        except Exception as e:
            print("error: ", str(e))
            self.closedb()
            return False

        if not self.ctx.transactions:
            self.ctx.commit()
        return True

    def get_parm(self, type):
        if type == tools.INT:
            return 0
        if type == tools.FLOAT:
            return 0.0
        if type == tools.STRING:
            return ""
        if type == tools.DATE or type == tools.DATETIME:
            return ""
        if type == tools.BINARY:
            return ""
        if type == tools.CURSOR:
            return self.db_module
        else:
            return ""
        
    def _process_insert_query(self, query, tablename, seqname):
        return query, SQLQuery('SELECT last_insert_id();')
        
    def _get_insert_default_values_query(self, table):
        return "INSERT INTO %s () VALUES()" % table


class DB2DB(DB):
    def __init__(self, **keywords):

        db = import_driver(["ibm_db"], preferred=keywords.pop('driver', None))
        if 'pw' in keywords:
            keywords['PWD'] = keywords['pw']
            del keywords['pw']
        if 'user' in keywords:
            keywords['UID'] = keywords['user']
            del keywords['user']
        if 'db' in keywords:
            keywords['DATABASE'] = keywords['db']
            del keywords['db']
        if 'host' in keywords:
            keywords['HOSTNAME'] = keywords['host']
            del keywords['host']
        if 'port' in keywords:
            keywords['PORT'] = keywords['port']
            del keywords['port']
        keywords['PROTOCOL'] = 'TCPIP'

        if 'charset' not in keywords:
            keywords['charset'] = 'utf8'
        elif keywords['charset'] is None:
            del keywords['charset']

        self.paramstyle = db.paramstyle = 'pyformat'  # it's both, like psycopg
        self.dbname = "db2"
        DB.__init__(self, db, keywords, self.dbname)
        self.supports_multiple_insert = True

    def _connect(self, keywords):
        conn = "DATABASE=%s;HOSTNAME=%s;PORT=%s;PROTOCOL=%s;UID=%s;PWD=%s;" % (keywords["DATABASE"], keywords["HOSTNAME"], keywords["PORT"], keywords["PROTOCOL"], keywords["UID"], keywords["PWD"])
        return self.db_module.connect(conn, "", "")

    def _load_context(self):
        self._ctx.dbq_count = 0
        self._ctx.transactions = []  # stack of transactions

        if self.has_pooling:
            print('*-*' * 25, 'create new db pooling', '*-*' * 25)
            self._ctx.db = self._connect_with_pooling(self.keywords)
        else:
            print('-*-' * 25, 'create new db connect:(%s)' % os.getpid(), '-*-' * 25)
            self._ctx.db = self._connect(self.keywords)
        self._ctx.db_execute = self._db_execute

        def commit(unload=False):
            # do db commit and release the connection if pooling is enabled.
            self.db_module.commit(self._ctx.db)
            if unload and self.has_pooling:
                self._unload_context()

        def rollback():
            # do db rollback and release the connection if pooling is enabled.
            self.db_module.rollback(self._ctx.db)
            if self.has_pooling:
                self._unload_context()

        self._ctx.commit = commit
        self._ctx.rollback = rollback

    def _db_execute(self, sql_query):
        """executes an sql query"""
        self.ctx.dbq_count += 1

        try:
            out = []
            a = time.time()
            query, params = self._process_query(sql_query)

            for i in range(len(params)):
                if isinstance(params[i], str):
                    params[i] = "'%s'" % params[i]

            # self.db_module.autocommit(self.ctx.db, self.db_module.SQL_AUTOCOMMIT_OFF)
            stmt = self.db_module.exec_immediate(self.ctx.db, query % tuple(params))
            if str(query).lstrip(" ").lower().startswith("select"):
                results = self.db_module.fetch_both(stmt)
                while results:
                    out.append(results)
                    results = self.db_module.fetch_both(stmt)
            else:
                out = self.db_module.num_rows(stmt)

            b = time.time()
        except:
            if self.printing:
                print('ERR:', str(query % tuple(params)), file=debug)
            try:
                if self.ctx.transactions:
                    self.ctx.transactions[-1].rollback()
                else:
                    self.ctx.rollback()
            except:
                try:
                    self.closedb()
                except:
                    raise
                raise
            try:
                self.closedb()
            except:
                raise
            raise

        if self.printing:
            print('%s (%s): %s' % (round(b - a, 2), self.ctx.dbq_count, str(query)), file=debug)
        return out

    def query(self, sql_query, vars=None, processed=False, _test=False):
        """
        Execute SQL query `sql_query` using dictionary `vars` to interpolate it.
        If `processed=True`, `vars` is a `reparam`-style list to use
        instead of interpolating.

            >>> db = DB(None, {})
            >>> db.query("SELECT * FROM foo", _test=True)
            <sql: 'SELECT * FROM foo'>
            >>> db.query("SELECT * FROM foo WHERE x = $x", vars=dict(x='f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
            >>> db.query("SELECT * FROM foo WHERE x = " + sqlquote('f'), _test=True)
            <sql: "SELECT * FROM foo WHERE x = 'f'">
        """
        if vars is None: vars = {}

        if not processed and not isinstance(sql_query, SQLQuery):
            sql_query = reparam(sql_query, vars)

        if _test: return sql_query

        try:
            results = self._db_execute(sql_query)
        except:
            raise
        if isinstance(results, int):
            return results
        if results is False or results == []:  # 查询结果为空
            out = []
        else:
            names = []
            datas = []
            for i in range(len(results)):
                data = []
                if i == 0:
                    for x in dict(results[0]).keys():
                        if isinstance(x, str):
                            names.append(x)
                for x in dict(results[0]).keys():
                    if isinstance(x, str):
                        data.append(results[i][x])
                datas.append(data)

            def iterwrapper(results):
                if results is None:
                    return
                for row in results:
                    yield storage(dict(zip(names, row)))

            out = iterbetter(iterwrapper(datas))
            out.__len__ = lambda: int(len(datas))
            out.list = lambda: [storage(dict(zip(names, x))) \
                                for x in datas]

        if not self.ctx.transactions:
            self.ctx.commit()
        return out

    def insert(self, tablename, seqname=None, _test=False, **values):
        """
        Inserts `values` into `tablename`. Returns current sequence ID.
        Set `seqname` to the ID if it's not the default, or to `False`
        if there isn't one.

            >>> db = DB(None, {})
            >>> q = db.insert('foo', name='bob', age=2, created=SQLLiteral('NOW()'), _test=True)
            >>> q
            <sql: "INSERT INTO foo (age, created, name) VALUES (2, NOW(), 'bob')">
            >>> q.query()
            'INSERT INTO foo (age, created, name) VALUES (%s, NOW(), %s)'
            >>> q.values()
            [2, 'bob']
        """

        def q(x):
            return "(" + x + ")"

        if values:
            # needed for Py3 compatibility with the above doctests
            sorted_values = sorted(values.items(), key=lambda t: t[0])

            _keys = SQLQuery.join(map(lambda t: t[0], sorted_values), ', ')
            _values = SQLQuery.join([sqlparam(v) for v in map(lambda t: t[1], sorted_values)], ', ')
            sql_query = "INSERT INTO %s " % tablename + q(_keys) + ' VALUES ' + q(_values)
        else:
            sql_query = SQLQuery(self._get_insert_default_values_query(tablename))

        if _test: return sql_query

        try:
            out = self._db_execute(sql_query)
        except:
            raise

        if not self.ctx.transactions:
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()

        return out

    def multiple_insert(self, tablename, values, seqname=None, _test=False):
        """
        Inserts multiple rows into `tablename`. The `values` must be a list of dictioanries,
        one for each row to be inserted, each with the same set of keys.
        Returns the list of ids of the inserted rows.
        Set `seqname` to the ID if it's not the default, or to `False`
        if there isn't one.

            >>> db = DB(None, {})
            >>> db.supports_multiple_insert = True
            >>> values = [{"name": "foo", "email": "foo@example.com"}, {"name": "bar", "email": "bar@example.com"}]
            >>> db.multiple_insert('person', values=values, _test=True)
            <sql: "INSERT INTO person (email, name) VALUES ('foo@example.com', 'foo'), ('bar@example.com', 'bar')">
        """
        if not values:
            return []

        if not self.supports_multiple_insert:
            print('not not not notttttttttttttttttttttttttttttttttt')
            try:
                out = [self.insert(tablename, seqname=seqname, _test=_test, **v) for v in values]
            except:
                raise
            if seqname is False:
                return None
            else:
                return out

        keys = values[0].keys()
        # @@ make sure all keys are valid

        for v in values:
            if v.keys() != keys:
                raise ValueError('Not all rows have the same keys')

        keys = sorted(keys)  # enforce query order for the above doctest compatibility with Py3

        sql_query = SQLQuery('INSERT INTO %s (%s) VALUES ' % (tablename, ', '.join(keys)))

        for i, row in enumerate(values):
            if i != 0:
                sql_query.append(", ")
            SQLQuery.join([SQLParam(row[k]) for k in keys], sep=", ", target=sql_query, prefix="(", suffix=")")

        if _test: return sql_query

        try:
            out = self._db_execute(sql_query)
        except:
            raise

        if not self.ctx.transactions:
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()

        return out

    def update(self, tables, where, vars=None, _test=False, **values):
        """
        Update `tables` with clause `where` (interpolated using `vars`)
        and setting `values`.

            >>> db = DB(None, {})
            >>> name = 'Joseph'
            >>> q = db.update('foo', where='name = $name', name='bob', age=2,
            ...     created=SQLLiteral('NOW()'), vars=locals(), _test=True)
            >>> q
            <sql: "UPDATE foo SET age = 2, created = NOW(), name = 'bob' WHERE name = 'Joseph'">
            >>> q.query()
            'UPDATE foo SET age = %s, created = NOW(), name = %s WHERE name = %s'
            >>> q.values()
            [2, 'bob', 'Joseph']
        """
        if vars is None: vars = {}
        where = self._where(where, vars)

        values = sorted(values.items(), key=lambda t: t[0])

        query = (
                "UPDATE " + sqllist(tables) +
                " SET " + sqlwhere(values, ', ') +
                " WHERE " + where)

        if _test: return query

        try:
            out = self._db_execute(query)
        except:
            raise

        if not self.ctx.transactions:
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()

        return out

    def delete(self, table, where, using=None, vars=None, _test=False):
        """
        Deletes from `table` with clauses `where` and `using`.

            >>> db = DB(None, {})
            >>> name = 'Joe'
            >>> db.delete('foo', where='name = $name', vars=locals(), _test=True)
            <sql: "DELETE FROM foo WHERE name = 'Joe'">
        """
        if vars is None: vars = {}
        where = self._where(where, vars)

        q = 'DELETE FROM ' + table
        if using: q += ' USING ' + sqllist(using)
        if where: q += ' WHERE ' + where

        if _test: return q

        try:
            out = self._db_execute(q)
        except:
            raise

        if not self.ctx.transactions:
            self.ctx.commit()

        if isinstance(out, int) and out < 0:
            self.closedb()

        return out

    def callproc(self, name, parmin=[], parmout=[], has_return=False):
        len_parmin = len(parmin)
        len_parmout = len(parmout)

        pp = parmin

        contain = False
        if parmout.__contains__(tools.CURSOR):
            contain = True
        else:
            contain = False

        # 解析tools参数到sql参数
        for i in range(len_parmout):
            if parmout[i] == tools.CURSOR:
                continue
            pp.append(self.get_parm(parmout[i]))

        server = self.db_module.server_info(self.ctx.db)

        try:
            ss = self.db_module.callproc(self.ctx.db, name, tuple(pp))

            if isinstance(ss, tuple):
                stmt = ss[0]
            else:
                stmt = ss

            results = []
            if contain and stmt is not None:
                if server.DBMS_NAME[0:3] != 'IDS':
                    row = self.db_module.fetch_tuple(stmt)  # fetch_both可获取到带列名的字典
                    while row:
                        results.append(row)
                        row = self.db_module.fetch_tuple(stmt)

            count = 0
            for i in range(len_parmout):
                if parmout[i] == tools.CURSOR:
                    parmout[i] = results
                    count = count + 1
                    continue
                parmout[i] = ss[len_parmin + 1 + i - count]

        except Exception as e:
            print("error: ", str(e))
            self.closedb()
            return False

        if not self.ctx.transactions:
            self.ctx.commit()
        return True

    def get_parm(self, type):
        if type == tools.INT:
            return 0
        if type == tools.FLOAT:
            return 0.0
        if type == tools.STRING:
            return ""
        if type == tools.DATE or type == tools.DATETIME:
            return ""
        if type == tools.BINARY:
            return ""
        if type == tools.CURSOR:
            return self.db_module
        else:
            return ""

    def closedb(self):
        try:
            settings.CTXS.pop(self.dbmark)
            if self._ctx.db:
                self.db_module.close(self._ctx.db)
        except:
            raise

    def _process_insert_query(self, query, tablename, seqname):
        return query, SQLQuery('SELECT last_insert_id();')

    def _get_insert_default_values_query(self, table):
        return "INSERT INTO %s () VALUES()" % table


def import_driver(drivers, preferred=None):
    """Import the first available driver or preferred driver.
    """
    if preferred:
        drivers = [preferred]

    for d in drivers:
        try:
            return __import__(d, None, None, ['x'])
        except ImportError:
            pass
    raise ImportError("Unable to import " + " or ".join(drivers))


class SqliteDB(DB): 
    def __init__(self, **keywords):
        db = import_driver(["sqlite3", "pysqlite2.dbapi2", "sqlite"], preferred=keywords.pop('driver', None))

        if db.__name__ in ["sqlite3", "pysqlite2.dbapi2"]:
            db.paramstyle = 'qmark'
            
        # sqlite driver doesn't create datatime objects for timestamp columns unless `detect_types` option is passed.
        # It seems to be supported in sqlite3 and pysqlite2 drivers, not surte about sqlite.
        keywords.setdefault('detect_types', db.PARSE_DECLTYPES)

        self.paramstyle = db.paramstyle
        keywords['database'] = keywords.pop('db')
        keywords['pooling'] = False # sqlite don't allows connections to be shared by threads
        self.dbname = "sqlite"        
        DB.__init__(self, db, keywords)

    def _process_insert_query(self, query, tablename, seqname):
        return query, SQLQuery('SELECT last_insert_rowid();')
    
    def query(self, *a, **kw):
        out = DB.query(self, *a, **kw)
        if isinstance(out, iterbetter):
            del out.__len__
        return out


class FirebirdDB(DB):
    """Firebird Database.
    """
    def __init__(self, **keywords):
        try:
            import kinterbasdb as db
        except Exception:
            db = None
            pass
        if 'pw' in keywords:
            keywords['password'] = keywords.pop('pw')
        keywords['database'] = keywords.pop('db')

        self.paramstyle = db.paramstyle

        DB.__init__(self, db, keywords)
        
    def delete(self, table, where=None, using=None, vars=None, _test=False):
        # firebird doesn't support using clause
        using=None
        return DB.delete(self, table, where, using, vars, _test)

    def sql_clauses(self, what, tables, where, group, order, limit, offset):
        return (
            ('SELECT', ''),
            ('FIRST', limit),
            ('SKIP', offset),
            ('', what),
            ('FROM', sqllist(tables)),
            ('WHERE', where),
            ('GROUP BY', group),
            ('ORDER BY', order)
        )


class MSSQLDB(DB):
    def __init__(self, **keywords):
        import pymssql as db    
        if 'pw' in keywords:
            keywords['password'] = keywords.pop('pw')
        keywords['database'] = keywords.pop('db')
        self.dbname = "mssql"
        DB.__init__(self, db, keywords, self.dbname)

    def _process_query(self, sql_query):
        """Takes the SQLQuery object and returns query string and parameters.
        """
        # MSSQLDB expects params to be a tuple. 
        # Overwriting the default implementation to convert params to tuple.
        paramstyle = getattr(self, 'paramstyle', 'pyformat')
        query = sql_query.query(paramstyle)
        params = sql_query.values()
        return query, tuple(params)

    def sql_clauses(self, what, tables, where, group, order, limit, offset):
        if limit is None:
            return (
                ('SELECT', what),
                ("TOP", limit),
                ('FROM', sqllist(tables)),
                ('WHERE', where),
                ('GROUP BY', group),
                ('ORDER BY', order),
                ('OFFSET', offset))
        else:
            return (
                ('SELECT TOP', ' %d %s ' % (limit, what)),
                ('FROM', sqllist(tables)),
                ('WHERE', where),
                ('GROUP BY', group),
                ('ORDER BY', order),
                ('OFFSET', offset))
            
    def _test(self):
        """Test LIMIT.

            Fake presence of pymssql module for running tests.
            >>> import sys
            >>> sys.modules['pymssql'] = sys.modules['sys']
            
            MSSQL has TOP clause instead of LIMIT clause.
            >>> db = MSSQLDB(db='test', user='joe', pw='secret')
            >>> db.select('foo', limit=4, _test=True)
            <sql: 'SELECT * TOP 4 FROM foo'>
        """
        pass

    def callproc(self, name, parmin=[], parmout=[], has_return=False):
        len_parmin = len(parmin)
        len_parmout = len(parmout)

        pp = parmin

        contain = False
        if parmout.__contains__(tools.CURSOR):
            contain = True
        else:
            contain = False

        # 解析tools参数到sql参数
        for i in range(len_parmout):
            if parmout[i] == tools.CURSOR:
                continue
            pp.append(self.get_parm(parmout[i]))

        cursor = self._db_cursor()

        try:
            results = []
            ret = cursor.callproc(name, tuple(pp))
            if contain:
                cursor.nextset()
                try:
                    results = cursor.fetchall()
                except Exception as e:
                    print(e)
                    pass
            if (contain and len_parmout > 1) or (not contain and len_parmout > 0):
                cur_count = 0
                for i in range(len(parmout)):
                    if parmout[i] == tools.CURSOR:
                        cur_count = cur_count + 1
                        continue
                    parmout[i] = ret[i - cur_count + len_parmin]

            if contain:
                for i in range(len_parmout):
                    if parmout[i] == tools.CURSOR:
                        parmout[i] = results

        except Exception as e:
            print("error: ", str(e))
            self.closedb()
            return False

        if not self.ctx.transactions:
            self.ctx.commit()
        return True

    def get_parm(self, type):
        if type == tools.INT:
            return self.db_module.output(int)
        if type == tools.FLOAT:
            return self.db_module.output(float)
        if type == tools.STRING:
            return self.db_module.output(str)
        if type == tools.DATE or type == tools.DATETIME:
            return self.db_module.output(str)
        if type == tools.BINARY:
            return self.db_module.output(str)
        if type == tools.CURSOR:
            return self.db_module
        if type == tools.DECIMAL:
            return self.db_module.output(float)
        else:
            return ""


class OracleDB(DB): 
    def __init__(self, **keywords): 
        import cx_Oracle as db 
        if 'pw' in keywords: 
            keywords['password'] = keywords.pop('pw') 

        #@@ TODO: use db.makedsn if host, port is specified
        dsn = db.makedsn(keywords.pop('host'), keywords.pop('port'), keywords.pop('service'))
        keywords['dsn'] = dsn
        keywords.pop('db')
        self.dbname = 'oracle' 
        db.paramstyle = 'numeric' 
        self.paramstyle = db.paramstyle

        # oracle doesn't support pooling 
        # keywords.pop('pooling', None)
        keywords['pooling'] = False
        DB.__init__(self, db, keywords, self.dbname)

    def _process_insert_query(self, query, tablename, seqname): 
        if seqname is None: 
            # It is not possible to get seq name from table name in Oracle
            return query
        else:
            return query + "; SELECT %s.currval FROM dual" % seqname

    def sql_clauses(self, what, tables, where, group, order, limit, offset):
        if limit is not None:
            if where is None:
                return (
                    ('SELECT', what),
                    ('FROM', sqllist(tables)),
                    ('WHERE', where),
                    ("WHERE ROWNUM<=", limit),
                    ('GROUP BY', group),
                    ('ORDER BY', order),
                    ('OFFSET', offset))
            else:
                return (
                    ('SELECT', what),
                    ('FROM', sqllist(tables)),
                    ('WHERE', where),
                    ("AND ROWNUM<=", limit),
                    ('GROUP BY', group),
                    ('ORDER BY', order),
                    ('OFFSET', offset))

    def callproc(self, name, parmin=[], parmout=[], has_return=False):
        len_parmin = len(parmin)
        len_parmout = len(parmout)

        pp = parmin

        contain = False
        if parmout.__contains__(tools.CURSOR):
            contain = True
        else:
            contain = False

        # 解析tools参数到oracle参数
        for i in range(len(parmout)):
            pp.append(self.get_parm(parmout[i]))

        cursor = self._db_cursor()

        try:
            cursor.callproc(name, pp)
        except Exception as e:
            self.closedb()
            print(str(e))
            return False
        for i in range(len_parmout):
            if parmout[i] == tools.CURSOR:
                parmout[i] = pp[i + len_parmin].getvalue()
                parmout[i] = parmout[i].fetchall()
            else:
                parmout[i] = pp[i + len_parmin].getvalue()

        if not self.ctx.transactions:
            self.ctx.commit()
        return True

    def get_parm(self, type):
        if type == tools.INT:
            return self._db_cursor().var(self.db_module.NATIVE_INT)
        if type == tools.FLOAT:
            return self._db_cursor().var(self.db_module.NUMBER)
        if type == tools.STRING:
            return self._db_cursor().var(self.db_module.STRING)
        if type == tools.DATE or type == tools.DATETIME:
            return self._db_cursor().var(self.db_module.DATETIME)
        if type == tools.BINARY:
            return self._db_cursor().var(self.db_module.BINARY)
        if type == tools.DECIMAL:
            return self._db_cursor().var(self.db_module.DECIMAL)
        if type == tools.CURSOR:
            return self._db_cursor().var(self.db_module.CURSOR)
        else:
            return self._db_cursor().var(self.db_module.STRING)


def dburl2dict(url):
    """
    Takes a URL to a database and parses it into an equivalent dictionary.
    
        >>> dburl2dict('postgres:///mygreatdb') == {'pw': None, 'dbn': 'postgres', 'db': 'mygreatdb', 'host': None, 'user': None, 'port': None}
        True
        >>> dburl2dict('postgres://james:day@serverfarm.example.net:5432/mygreatdb') == {'pw': 'day', 'dbn': 'postgres', 'db': 'mygreatdb', 'host': 'serverfarm.example.net', 'user': 'james', 'port': 5432}
        True
        >>> dburl2dict('postgres://james:day@serverfarm.example.net/mygreatdb') == {'pw': 'day', 'dbn': 'postgres', 'db': 'mygreatdb', 'host': 'serverfarm.example.net', 'user': 'james', 'port': None}
        True
        >>> dburl2dict('postgres://james:d%40y@serverfarm.example.net/mygreatdb') == {'pw': 'd@y', 'dbn': 'postgres', 'db': 'mygreatdb', 'host': 'serverfarm.example.net', 'user': 'james', 'port': None}
        True
        >>> dburl2dict('mysql://james:d%40y@serverfarm.example.net/mygreatdb') == {'pw': 'd@y', 'dbn': 'mysql', 'db': 'mygreatdb', 'host': 'serverfarm.example.net', 'user': 'james', 'port': None}
        True
    """
    parts = urlparse.urlparse(unquote(url))

    return {'dbn': parts.scheme,
            'user': parts.username,
            'pw': parts.password,
            'db': parts.path[1:],
            'host': parts.hostname,
            'port': parts.port}


_databases = {}


def database(dburl=None, **params):
    """Creates appropriate database using params.
    
    Pooling will be enabled if DBUtils module is available. 
    Pooling can be disabled by passing pooling=False in params.
    """
    if not dburl and not params:
        dburl = os.environ['DATABASE_URL']
    if dburl:
        params = dburl2dict(dburl)
    # 取消pooling
    params['pooling'] = False
    dbn = params.pop('dbn')
    if dbn in _databases:
        return _databases[dbn](**params)
    else:
        raise UnknownDB(dbn)


def register_database(name, clazz):
    """
    Register a database.

        >>> class LegacyDB(DB): 
        ...     def __init__(self, **params): 
        ...        pass 
        ...
        >>> register_database('legacy', LegacyDB)
        >>> db = database(dbn='legacy', db='test', user='joe', passwd='secret') 
    """
    _databases[name] = clazz


register_database('mysql', MySQLDB)
register_database('postgres', PostgresDB)
register_database('sqlite', SqliteDB)
register_database('firebird', FirebirdDB)
register_database('mssql', MSSQLDB)
register_database('oracle', OracleDB)
register_database('db2', DB2DB)


def _interpolate(format): 
    """
    Takes a format string and returns a list of 2-tuples of the form
    (boolean, string) where boolean says whether string should be evaled
    or not.

    from <http://lfw.org/python/Itpl.py> (public domain, Ka-Ping Yee)
    """
    def matchorfail(text, pos):
        match = tokenprog.match(text, pos)
        if match is None:
            raise _ItplError(text, pos)
        return match, match.end()

    namechars = "abcdefghijklmnopqrstuvwxyz" \
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
    chunks = []
    pos = 0

    while 1:
        dollar = format.find("$", pos)
        if dollar < 0: 
            break
        nextchar = format[dollar + 1]

        if nextchar == "{":
            chunks.append((0, format[pos:dollar]))
            pos, level = dollar + 2, 1
            while level:
                match, pos = matchorfail(format, pos)
                tstart, tend = match.regs[3]
                token = format[tstart:tend]
                if token == "{": 
                    level = level + 1
                elif token == "}":  
                    level = level - 1
            chunks.append((1, format[dollar + 2:pos - 1]))

        elif nextchar in namechars:
            chunks.append((0, format[pos:dollar]))
            match, pos = matchorfail(format, dollar + 1)
            while pos < len(format):
                if format[pos] == "." and pos + 1 < len(format) and format[pos + 1] in namechars:
                    match, pos = matchorfail(format, pos + 1)
                elif format[pos] in "([":
                    pos, level = pos + 1, 1
                    while level:
                        match, pos = matchorfail(format, pos)
                        tstart, tend = match.regs[3]
                        token = format[tstart:tend]
                        if token[0] in "([": 
                            level = level + 1
                        elif token[0] in ")]":  
                            level = level - 1
                else: 
                    break
            chunks.append((1, format[dollar + 1:pos]))
        else:
            chunks.append((0, format[pos:dollar + 1]))
            pos = dollar + 1 + (nextchar == "$")

    if pos < len(format): 
        chunks.append((0, format[pos:]))
    return chunks


class _Node(object):
    def __init__(self, type, first, second=None):
        self.type = type
        self.first = first
        self.second = second

    def __eq__(self, other):
        return (isinstance(other, _Node)
            and self.type == other.type
            and self.first == other.first
            and self.second == other.second)

    def __repr__(self):
        return "Node(%r, %r, %r)" % (self.type, self.first, self.second)


class Parser:
    """Parser to parse string templates like "Hello $name".

    Loosely based on <http://lfw.org/python/Itpl.py> (public domain, Ka-Ping Yee)
    """
    namechars = "abcdefghijklmnopqrstuvwxyz" \
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"

    def __init__(self):
        self.reset()

    def reset(self):
        self.pos = 0
        self.level = 0
        self.text = ""

    def parse(self, text):
        """Parses the given text and returns a parse tree.
        """
        self.reset()
        self.text = text
        return self.parse_all()

    def parse_all(self):
        while True:
            dollar = self.text.find("$", self.pos)
            if dollar < 0:
                break
            nextchar = self.text[dollar + 1]
            if nextchar in self.namechars:
                yield _Node("text", self.text[self.pos:dollar])
                self.pos = dollar+1
                yield self.parse_expr()

            # for supporting ${x.id}, for backward compataility
            elif nextchar == '{':
                saved_pos = self.pos
                self.pos = dollar+2 # skip "${"
                expr = self.parse_expr()
                if self.text[self.pos] == '}':
                    self.pos += 1
                    yield _Node("text", self.text[self.pos:dollar])
                    yield expr
                else:
                    self.pos = saved_pos
                    break
            else:
                yield _Node("text", self.text[self.pos:dollar+1])
                self.pos = dollar + 1
                # $$ is used to escape $
                if nextchar == "$":
                    self.pos += 1

        if self.pos < len(self.text):
            yield _Node("text", self.text[self.pos:])

    def match(self):
        match = tokenprog.match(self.text, self.pos)
        if match is None:
            raise _ItplError(self.text, self.pos)
        return match, match.end()

    def is_literal(self, text):
        return text and text[0] in "0123456789\"'"

    def parse_expr(self):
        match, pos = self.match()
        if self.is_literal(match.group()):
            expr = _Node("literal", match.group())
        else:
            expr = _Node("param", self.text[self.pos:pos])
        self.pos = pos
        while self.pos < len(self.text):
            if self.text[self.pos] == "." and self.pos + 1 < len(self.text) and self.text[self.pos + 1] in self.namechars:
                self.pos += 1
                match, pos = self.match()
                attr = match.group()
                expr = _Node("getattr", expr, attr)
                self.pos = pos
            elif self.text[self.pos] == "[":
                saved_pos = self.pos
                self.pos += 1
                key = self.parse_expr()
                if self.text[self.pos] == ']':
                    self.pos += 1
                    expr = _Node("getitem", expr, key)
                else:
                    self.pos = saved_pos
                    break
            else:
                break
        return expr


class SafeEval(object):
    """Safe evaluator for binding params to db queries.
    """
    def safeeval(self, text, mapping):
        nodes = Parser().parse(text)
        return SQLQuery.join([self.eval_node(node, mapping) for node in nodes], "")

    def eval_node(self, node, mapping):
        if node.type == "text":
            return node.first
        else:
            return sqlquote(self.eval_expr(node, mapping))

    def eval_expr(self, node, mapping):
        if node.type == "literal":
            return ast.literal_eval(node.first)
        elif node.type == "getattr":
            return getattr(self.eval_expr(node.first, mapping), node.second)
        elif node.type == "getitem":
            return self.eval_expr(node.first, mapping)[self.eval_expr(node.second, mapping)]
        elif node.type == "param":
            return mapping[node.first]


def test_parser():
    def f(text, expected):
        p = Parser()
        nodes = list(p.parse(text))
        print(repr(text), nodes)
        assert nodes == expected, "Expected %r" % expected

    f("Hello", [_Node("text", "Hello")])
    f("Hello $name", [_Node("text", "Hello "), _Node("param", "name")])
    f("Hello $name.foo", [
        _Node("text", "Hello "),
        _Node("getattr",
            _Node("param", "name"),
            "foo")])
    f("WHERE id=$self.id LIMIT 1", [
        _Node("text", "WHERE id="),
        _Node('getattr',
            _Node('param', 'self', None),
            'id'),
        _Node("text", " LIMIT 1")])

    f("WHERE id=$self['id'] LIMIT 1", [
        _Node("text", "WHERE id="),
        _Node('getitem',
            _Node('param', 'self', None),
            _Node('literal', "'id'")),
        _Node("text", " LIMIT 1")])


def test_safeeval():
    def f(q, vars):
        return SafeEval().safeeval(q, vars)

    print(f("WHERE id=$id", {"id": 1}).items)
    assert f("WHERE id=$id", {"id": 1}).items == ["WHERE id=", sqlparam(1)]


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    test_parser()
    test_safeeval()
