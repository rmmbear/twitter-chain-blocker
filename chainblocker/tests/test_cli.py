import sys
import logging
import tempfile
from types import SimpleNamespace
from pathlib import Path

from typing import Any, Dict, Optional, Union

import sqlalchemy as sqla
from sqlalchemy.orm import Session, sessionmaker

from chainblocker import BlocklistDBBase
from chainblocker import __main__ as cli

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)


class DummyTwitterUser():
    """Dummy twitter user
    When used in a with statement, it automatically replaces the
    cli.authenticate_interactive with this class, and then reimports the
    cli module on exit.
    """
    id = 0
    original_function = cli.authenticate_interactive
    users: Dict[str, "DummyTwitterUser"] = {}


    def __init__(self, name: Optional[str] = None) -> None:
        self.__class__.id += 1
        LOGGER.debug("Creating new DummyTwitterUser with id %s", self.id)

        self.id = self.__class__.id
        if name:
            self.screen_name = name
        else:
            self.screen_name = f"Dummy User {self.id}"

        self.followers_count = sys.maxsize
        self.friends_count = sys.maxsize


    def __enter__(self) -> "DummyTwitterUser":
        self.__class__.users = {}
        self.__class__._id = 0
        cli.authenticate_interactive = self.__class__
        return self


    def __exit__(self, *exc: Any) -> None:
        cli.authenticate_interactive = self.__class__.original_function


    @property
    def user(self):
        user_namespace = SimpleNamespace(
            id=self.id,
            screen_name=self.screen_name,
            followers_count=self.followers_count,
            friends_count=self.friends_count
        )
        return user_namespace


    #FIXME: Implement all other methods
    def get_user_by_name(self, screen_name: str) -> "DummyTwitterUser":
        if screen_name not in self.__class__.users:
            new_user = self.__class__(screen_name)
            self.__class__.users[screen_name] = new_user

        return self.__class__.users[screen_name]



class DummyBlocklistDBSession():
    """Use explicitly only with the "with" statment.
    The idea is that this will return an instance of itself only in the
    body of a test function, and all other calls (which should be
    through the overloaded cli.create_db_session) will result in the
    same session object being returned.
    """
    original_function = cli.create_db_session
    current_session: Optional[Session] = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Union[Session, "DummyBlocklistDBSession"]:
        """Return current_session if it's set, otherwise create a new object."""
        if cls.current_session is None:
            return super().__new__(cls)
        return cls.current_session


    def __init__(self) -> None:
        assert not self.current_session, "Only one instance should ever be active"

        LOGGER.info("Creating in-memory blocklist db session")
        sqla_engine = sqla.create_engine("sqlite:///:memory:", echo=True)
        BlocklistDBBase.metadata.create_all(sqla_engine)
        self.bound_session = sessionmaker(bind=sqla_engine)


    def __enter__(self) -> Session:
        """Create the session and overload cli"""
        cli.create_db_session = self.__class__
        self.__class__.current_session = self.bound_session()
        return self


    def __exit__(self, *exc: Any) -> None:
        """"""
        self.__class__.current_session.close()
        self.__class__.current_session = None
        cli.create_db_session = self.__class__.original_function



###
### ACTUAL TESTS START HERE
###
def test_db_init() -> None:
    """Test initialization of a new empty db file on disk"""
    LOGGER.info("STARTING: DB init test")

    with DummyTwitterUser() as dummy, \
         tempfile.TemporaryDirectory() as tempdir:
        cli.main(paths=cli.get_workdirs(home=Path(tempdir)), args="")

    LOGGER.info("COMPLETED: DB init test")


def test_unblock() -> None:
    """"""
    LOGGER.info("STARTING: unblocking test")
    #FIXME: This needs setting up
    with DummyTwitterUser() as u_dummy, \
         DummyBlocklistDBSession() as db_dummy, \
         tempfile.TemporaryDirectory() as tempdir:
        cli.main(paths=cli.get_workdirs(home=Path(tempdir)),
                 args="unblock user".split())

    LOGGER.info("COMPLETED: unblocking test")


def test_block() -> None:
    """"""
    LOGGER.info("STARTING: blocking test")
    #FIXME: This needs setting up
    with DummyTwitterUser() as u_dummy, \
         DummyBlocklistDBSession() as db_dummy, \
         tempfile.TemporaryDirectory() as tempdir:
        cli.main(paths=cli.get_workdirs(home=Path(tempdir)),
                 args="block user".split())

    LOGGER.info("COMPLETED: blocking test")
