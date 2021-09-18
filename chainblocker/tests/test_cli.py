import logging
import tempfile
from types import SimpleNamespace
from pathlib import Path

from typing import Any, Dict, Optional, Generator, Iterable, List, Tuple

import sqlalchemy as sqla
from sqlalchemy.orm import Session, sessionmaker

import pytest
from tweepy.models import User

from chainblocker import BlocklistDBBase, BlockList, BlockQueue, UnblockQueue
from chainblocker import __main__ as cli

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.DEBUG)


class DummyTwitterUser():
    """Dummy twitter user
    When used in a with statement, it automatically replaces the
    cli.authenticate_interactive with this class, and then reimports the
    cli module on exit.
    """
    user_id = 0
    original_function = cli.authenticate_interactive
    users: Dict[str, "DummyTwitterUser"] = {}
    users_int: Dict[int, "DummyTwitterUser"] = {}


    #TODO: implement creation of dummies with pre-defined ids
    def __init__(self, name: Optional[str] = None,
                 followers: Tuple[int, int] = 100, following: int = 100, blocked: int = 100
                ) -> None:
        """"""
        self.__class__.user_id += 1
        self.user_id = self.__class__.user_id
        LOGGER.debug("Creating new DummyTwitterUser with id %s", self.user_id)

        if name:
            self.screen_name = name
        else:
            self.screen_name = f"Dummy_{self.user_id}"
        self.name = self.screen_name

        #FIXME: implement creation of followers and followed accounts
        self.follower_ids: List[int] = []
        self.followed_ids: List[int] = []
        self.blocked_ids: List[int] = []
        self.followers_count = len(self.follower_ids)
        self.friends_count = len(self.followed_ids)

        self.user = SimpleNamespace(
            id=self.user_id,
            screen_name=self.screen_name,
            followers_count=self.followers_count,
            friends_count=self.friends_count
        )

        self.__class__.users[self.name] = self
        self.__class__.users_int[self.user_id] = self


    @property
    def id(self):
        return self.user_id


    #FIXME: Implement all other methods
    @classmethod
    def get_user_by_name(cls, screen_name: str, create=True) -> "DummyTwitterUser":
        """Return dummy object, create one if name is not found"""
        if screen_name not in cls.users:
            if create:
                LOGGER.debug("User '%s' not found, creating new dummy account", screen_name)
                cls(screen_name)
            else:
                raise RuntimeError(f"User '{screen_name}' not found")

        return cls.users[screen_name]


    @classmethod
    def get_user_by_id(cls, user_id: int, create=True) -> "DummyTwitterUser":
        """"""
        if user_id not in cls.users_int:
            if create:
                LOGGER.debug("User id '%s' not found, creating new dummy account", user_id)
                raise NotImplementedError("Creation of users with known id unsupported")
                cls(user_id)
            else:
                raise RuntimeError(f"User id '{user_id}' not found")

        return cls.users_int[user_id]



class DummyAuthedUser(DummyTwitterUser):
    def __init__(self, name: Optional[str] = None):
        super().__init__(name)

        self.api = SimpleNamespace(
            create_block=self._api_create_block,
            #blocks_ids=self._api_blocks_ids,
            #destroy_blocks=self._api_destroy_blocks,
            #followers_ids=self._api_followers_ids,
            #friends_ids=self._api_friends_ids,
            #get_user=self._api_get_user,
            #me=self._api_me,
            #rate_limit_status=self._api_rate_limit_status
        )


    def __enter__(self) -> "DummyTwitterUser":
        cli.authenticate_interactive = self.__class__
        return self


    def __exit__(self, *exc: Any) -> None:
        cli.authenticate_interactive = self.__class__.original_function
        self.__class__.users = {}
        self.__class__.users_int = {}
        self.__class__.user_id = 0


    # the following need to be overloaded
    # TODO: api.create_block(user_id=queued_block.user_id)
    # TODO: api.blocks_ids
    # TODO: api.destroy_blocks
    # TODO: api.followers_ids
    # TODO: api.friends_ids
    # TODO: api.get_user(screen_name=screen_name)
    # TODO: api.get_user(user_id=user_id)
    # TODO: api.me()
    # TODO: api.rate_limit_status()
    #

    def _api_blocks_ids(self) -> Iterable[int]:
        return self.blocked_ids


    def _api_create_block(self, *args, user_id: int, **kwargs) -> User:
        LOGGER.debug("blocking user %s", user_id)
        return self.get_user_by_id(user_id)

    #def _api_destroy_block -> User:
    #def _api_followers_ids -> Iterable[int]:
    #def _api_friends_ids -> Iterable[int]:
    #def _api_get_user(*args, screen_name=None, user_id=None) -> User:
    #def _api_me(self) -> User:
    #    return self.user
    #def _api_rate_limit_status() -> Json


    # for best results, the tweepy cursoring method should also be overloaded
    # so that we only have to worry about overloading at one level of abstraction
    # but since cursoring is only explicitly used in the 'get_*id*' methods of TwitterUser,
    # the much easier and quicker approach is to overload those
    # tweepy.Cursor(self.api.followers_ids, user_id=user_id).pages()):

    def get_follower_ids(self) -> Generator[int, None, None]:
        """"""
        for user_id in self.follower_ids:
            yield user_id


    def get_follower_id_pages(self, page_limit: int = 1000) -> Generator[Iterable[int], None, None]:
        """"""
        first = 0
        last = page_limit
        while True:
            page = self.follower_ids[first:last]
            if not page:
                break

            yield page
            first += page_limit
            last += page_limit


    def get_followed_ids(self) -> Generator[int, None, None]:
        """"""
        for user_id in self.followed_ids:
            yield user_id


    def get_followed_id_pages(self, page_limit: int = 1000) -> Generator[List[int], None, None]:
        """"""
        first = 0
        last = page_limit
        while True:
            page = self.followed_ids[first:last]
            if not page:
                break

            yield page
            first += page_limit
            last += page_limit


    def get_blocked_id_pages(self, page_limit: int = 1000) -> Generator[List[int], None, None]:
        """"""
        first = 0
        last = page_limit
        while True:
            page = self.blocked_ids[first:last]
            if not page:
                break

            yield page
            first += page_limit
            last += page_limit



class DummyDBSession():
    """Use only through context manager.
    """
    original_function = cli.create_db_session
    current_session: Optional[Session] = None

    def __init__(self, in_memory: bool, override_path: Optional[str] = None) -> None:
        if not in_memory and not override_path:
            raise ValueError("Path must be specified when not creating in-memory db")

        self.dummy_accessed = 0
        self.dummy_in_memory: bool = in_memory
        self.dummy_override_path: Optional[str] = None
        self.dummy_dbfile: Optional[str] = None


    def __enter__(self) -> Session:
        """Create the session and overload cli"""
        cli.create_db_session = self.dummy_create_session
        #self.__class__.current_session = self.bound_session()
        return self


    def __exit__(self, *exc: Any) -> None:
        if not self.__class__.current_session is None:
            self.__class__.current_session.close()
            self.__class__.current_session = None

        cli.create_db_session = self.__class__.original_function


    def __getattr__(self, name):
        if name.startswith("dummy"):
            raise AttributeError(f"'{self.__class__.__name__}' has no attribute '{name}'")
        # all attributes of this class must be prefixed with "dummy" to avoid
        # accidentally accessing attributes of the session object
        #XXX: this is unnecessarily clever and should be removed if this class gets any more complex
        return getattr(self.__class__.current_session, name)


    def dummy_create_session(self, path: Path, name: str,
                             suffix: str = "_blocklist.sqlite"
                            ) -> Session:
        """"""
        LOGGER.debug("Creating db session: path=%s, name=%s, suffix=%s", path, name, suffix)
        LOGGER.debug("Overrides: in_memory=%s, path=%s",
                     self.dummy_in_memory, self.dummy_override_path
                    )
        self.dummy_accessed += 1

        if self.current_session:
            return self.current_session
        #    raise RuntimeError("ATTEMPTED TO CREATE MULTIPLE DB SESSIONS")
        #FIXME: should check if attempts are made to create a session when one already exists
        # in the context of caller's scope
        # i.e. if multiple sessions are being created when invoking cli functions

        if self.dummy_override_path:
            path = self.override_path

        if self.dummy_in_memory:
            dbfile = ":memory:"
        else:
            dbfile = path / f"{name}{suffix}"

        self.dummy_dbfile = dbfile

        LOGGER.debug("dbfile = %s", dbfile)
        sqla_engine = sqla.create_engine(f"sqlite:///{str(dbfile)}", echo=False)
        BlocklistDBBase.metadata.create_all(sqla_engine)
        bound_session = sessionmaker(bind=sqla_engine)

        self.__class__.current_session = bound_session()
        return self.__class__.current_session


###
### ACTUAL TESTS START HERE
###
def test_clean_start() -> None:
    """Verify that it is possible to create a fresh db on disk

    """
    with tempfile.TemporaryDirectory() as tempdir:
        with DummyAuthedUser("dummy") as u_dummy, \
             DummyDBSession(in_memory=False, override_path=tempdir) as db_dummy:

            LOGGER.info("STARTING: DB init test")
            cli.main(paths=cli.get_workdirs(home=Path(tempdir)), args="")
            assert db_dummy.dummy_accessed == 1
            LOGGER.info("COMPLETED: DB init test")

            LOGGER.info("STARTING: block test")
            block_target = u_dummy.get_user_by_name("test_block_target")
            cli.main(paths=cli.get_workdirs(home=Path(tempdir)),
                     args=f"block {block_target.screen_name}".split())
            assert db_dummy.dummy_accessed == 2

            amnt_queued = db_dummy.query(BlockQueue).count()
            assert amnt_queued == 0, \
                f"Block queue was not fully processed ({amnt_queued} still in queue)"

            amnt_blocked = db_dummy.query(BlockList).count()
            expected_blocked = len(block_target.follower_ids) + 1 # followers + target
            assert amnt_blocked == expected_blocked, \
                f"expected {expected_blocked} != blocked {amnt_blocked}"

            LOGGER.info("COMPLETED: block test")

            LOGGER.info("STARTING: unblock test")
            with pytest.raises(NotImplementedError):
                cli.main(paths=cli.get_workdirs(home=Path(tempdir)), args="unblock test_block_target".split())
                assert db_dummy.dummy_accessed == 3
                amnt_queued = db_dummy.query(UnblockQueue).count()
                assert amnt_queued == 0, \
                    f"Unblock queue was not fully processed ({amnt_queued} still in queue)"

                amnt_blocked = db_dummy.query(BlockList).count()
                assert amnt_blocked == 0, "did not unblock everyone"
            LOGGER.info("COMPLETED: unblock test")
