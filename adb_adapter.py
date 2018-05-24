import datetime
import aiopg
from logger import logger

LOG = logger.LOG


def now():
    return datetime.datetime.now()


class APgConnection(object):
    def __init__(self):
        self._db: aiopg.Connection = None

    async def aexecute(self, query: str, bind_params: tuple = None, timeout=10):
        def sqlprint():
            if bind_params:
                LOG.debug("SQL >\n\t> " + query.replace("%s", "%r") % bind_params)
            else:
                LOG.debug("SQL >\n\t> " + query)

        if not self._db:
            # Try to init by local setting. Useful for testing only
            await self.ainit("mycapp", "local_test", "test", "127.0.0.1")
        if not self._db:
            raise ValueError("Need to init connection before first use")
        async with self._db.cursor() as cursor:
            sqlprint()
            await cursor.execute(query, bind_params, timeout=timeout)
            ret = []
            if cursor.description:
                async for row in cursor:
                    ret.append(row)
            return ret

    async def ainit(self, dbname, user, passwd, host):
        if self._db:
            LOG.debug("Try to reinit current connection: {}".format(self._db))
        dsn = "dbname={} user={} password={} host={}".format(dbname, user, passwd, host)
        LOG.info("Connect to DB:{}".format(dsn))
        self._db = await aiopg.connect(dsn=dsn, timeout=10)

    def close(self):
        if self._db and not self._db.closed():
            LOG.debug("Try to close current connection: {}".format(self._db))
        else:
            raise ValueError("There is no connection to close")
        self._db.close()


dbpg = APgConnection()


# Testing
if __name__ == '__main__':
    query2 = "SELECT fiproduct_id, fiaccount_id, filimit_user_profiles, fistatus, fdcreation, fdclose_date FROM mycapp.test;"
    params = ("asdasd", 411165, now())
    params1 = ("sd", 65, now())
