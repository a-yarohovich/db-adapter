import psycopg2
import psycopg2.extensions
from logger import logger

LOG = logger.LOG


class LoggingCursor(psycopg2.extensions.cursor):
    def execute(self, sql: str, args=None):
        """
        :return: None
        :raise: not safe
        """
        if args:
            LOG.debug("SQL >\n\t> " + sql.replace("%s", "%r") % args, filepath_print_stack_level=-4)
        else:
            LOG.debug("SQL >\n\t> " + sql, filepath_print_stack_level=-4)
        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as exc:
            LOG.error("%s: %s" % (exc.__class__.__name__, exc))
            raise


class MyPgConnection(object):
    def __init__(self):
        self._db = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val:
            self.base_connection.rollback()
            raise exc_val
        else:
            self.base_connection.commit()

    def init(self, dbname, user, passwd, host):
        if self._db:
            LOG.debug("Try to reinit current connection: {}".format(self._db))
        dsn = "dbname={} user={} password={} host={}".format(dbname, user, passwd, host)
        LOG.info("Connect to DB:{}".format(dsn))
        self._db = psycopg2.connect(dsn=dsn)
        self._db.autocommit = False

    @property
    def cursor(self) -> psycopg2.extensions.cursor:
        """
        :return: pg cursor
        :raise: not safe
        """
        return self._db.cursor(name=None, cursor_factory=LoggingCursor, withhold=False)

    @property
    def base_connection(self):
        return self._db


if __name__ == "__main__":
    db = MyPgConnection()
    db.init(
        dbname="mycapp",
        user="local_test",
        passwd="test",
        host="localhost"
    )

    def get_role_by_name(fsname):
        with db.cursor as cur:
            cur.execute("SELECT * FROM mycapp.user_roles WHERE fsname=%s", (fsname,))
            data = cur.fetchone()
            if not data:
                return None
            return {"firole_id": data[0], "fsname": data[1], "fidefault_role": data[2], "fipermissions": data[3]}

    # Transaction example
    def tr_ex1():
        with db:
            cur = db.cursor
            cur.execute("INSERT INTO mycapp.test(fiaccount_id) VALUES (%s) RETURNING fiproduct_id", (1212,))
            fiprod_id = cur.fetchone()[0]
            cur.execute("INSERT INTO mycapp.test1(fiaccount_id) VALUES (%s) RETURNING fiproduct_id", (231387544,))
            fiprod_id1 = cur.fetchone()[0]
            return fiprod_id, fiprod_id1

    print(tr_ex1())
