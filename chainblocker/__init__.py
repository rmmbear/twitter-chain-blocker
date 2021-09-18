""""""
import time
import logging

from typing import Any, Generator, Iterable, List, Optional, Tuple

import tweepy
from tweepy.models import User

import sqlalchemy as sqla
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.decl_api import DeclarativeMeta

LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)
TH = logging.StreamHandler()
TH.setLevel(logging.DEBUG)
TH.setFormatter(LOG_FORMAT_TERM)

LOGGER.addHandler(TH)

BlocklistDBBase: DeclarativeMeta = declarative_base()


class RepeatUntilSuccess():
    success_count = 0
    err_count = 0
    err_timeout = 30
    err_timeout_incr = 10
    err_delay_mult = 0
    __slots__ = ("orig_attr",)

    def __init__(self, attr: Any) -> None:
        self.orig_attr = attr


    def __getattr__(self, name: str) -> Any:
        return self.orig_attr.__getattribute__(name)


    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        while True:
            try:
                ret = self.orig_attr(*args, **kwargs)
                self.__class__.success_count += 1
                if self.err_delay_mult and self.success_count >= 200:
                    self.__class__.err_delay_mult -= 1
                    self.__class__.success_count = 0

                return ret
            except tweepy.error.TweepError as err:
                LOGGER.error("Err: %s", err)
                if err.api_code is None:
                    sleep_time = self.err_timeout + (self.err_timeout_incr * self.err_delay_mult)
                    LOGGER.error("Sleeping for %s", sleep_time)
                    time.sleep(sleep_time)
                    self.__class__.err_count += 1
                    self.__class__.err_delay_mult += 1
                    self.__class__.success_count = 0
                    continue

                raise err


class API(tweepy.API):
    __slots__ = ()
    def __getattribute__(self, name: str) -> Any:
        attr = super().__getattribute__(name)
        repeatable = [
            "create_block", "get_user", "get_followed_ids",
            "friends_ids", "followers_ids", "blocks_ids"
        ]
        if name in (repeatable):
            attr = RepeatUntilSuccess(attr)

        return attr


class Metadata(BlocklistDBBase):
    """"""
    __tablename__ = "metadata"
    key = sqla.Column(sqla.String, primary_key=True)
    val = sqla.Column(sqla.String)

    @classmethod
    def get_row(cls, key_name: str, db_session: Session, default_val: str = "") -> "Metadata":
        """Find the row with matching key, or create it if it does not exists, and return it."""
        row = db_session.query(cls).filter(cls.key == key_name).one_or_none()
        if not row:
            row = cls(key=key_name, val=default_val)
            db_session.add(row)
            # expecting client functions to commit this change

        return row


    @classmethod
    def set_row(cls, key_name: str, value: Any, db_session: Session) -> "Metadata":
        """Set the value of row with matching key and return it.
        Creates the row if it does not yet exist.
        """
        row = cls.get_row(key_name, db_session)
        row.val = str(value)
        db_session.commit()
        return row


class BlockHistory(BlocklistDBBase):
    """"""
    __tablename__ = "history"
    id = sqla.Column(sqla.Integer, primary_key=True)
    session = sqla.Column(sqla.Integer)
    user_id = sqla.Column(sqla.Integer)
    screen_name = sqla.Column(sqla.String)
    followers = sqla.Column(sqla.Integer)
    following = sqla.Column(sqla.Integer)
    mode = sqla.Column(sqla.String) # this is either "block" or "unblock"
    affect_target = sqla.Column(sqla.Boolean)
    affect_followers = sqla.Column(sqla.Boolean)
    affect_followed = sqla.Column(sqla.Boolean)
    time = sqla.Column(sqla.Float)
    queued = sqla.Column(sqla.Integer)
    skipped_blocked = sqla.Column(sqla.Integer)
    skipped_queued = sqla.Column(sqla.Integer)
    skipped_following = sqla.Column(sqla.Integer)
    comment = sqla.Column(sqla.String)


class BlockList(BlocklistDBBase):
    """"""
    __tablename__ = "blocked_accounts"
    user_id = sqla.Column(sqla.Integer, primary_key=True)
    block_time = sqla.Column(sqla.Float)
    reason = sqla.Column(sqla.Integer) # 0: unknown, 1: target, 2: follower, 3: followed
    reason_id = sqla.Column(sqla.Integer) # id of user responsible for this block, none if reason=1
    session = sqla.Column(sqla.Integer) # id of existing BlockHistory row


class BlockQueue(BlocklistDBBase):
    """"""
    __tablename__ = "block_queue"
    user_id = sqla.Column(sqla.Integer, primary_key=True)
    queued_at = sqla.Column(sqla.Float)
    reason = sqla.Column(sqla.Integer) # 0: unknown, 1: target, 2: follower, 3: followed
    reason_id = sqla.Column(sqla.Integer) # id of user responsible for this block, none if reason=1
    session = sqla.Column(sqla.Integer) # id of existing BlockHistory row


class UnblockQueue(BlocklistDBBase):
    """"""
    __tablename__ = "unblock_queue"
    user_id = sqla.Column(sqla.Integer, primary_key=True)
    queued_at = sqla.Column(sqla.Float)
    reason = sqla.Column(sqla.Integer) # 0: unknown, 1: target, 2: follower, 3: followed
    reason_id = sqla.Column(sqla.Integer) # id of user responsible for this block, none if reason=1
    session = sqla.Column(sqla.Integer) # id of existing BlockHistory row


class TaskQueue(BlocklistDBBase):
    """"""
    __tablename__ = "task_queue"
    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.Integer)
    screen_name = sqla.Column(sqla.String)
    followers = sqla.Column(sqla.Integer)
    following = sqla.Column(sqla.Integer)
    action = sqla.Column(sqla.String) # "block/unblock"
    affect_target = sqla.Column(sqla.Boolean)
    affect_followers = sqla.Column(sqla.Boolean)
    affect_followed = sqla.Column(sqla.Boolean)
    comment = sqla.Column(sqla.String)


class AuthedUser:
    """"""
    # Note that inclusion of api keys below is intentional
    # even desktop apps must use twitter's api keys for authentication,
    # meaning that the only options for me are:
    # 1. Create and maintain some sort of proxy authentication server
    # 2. Include the keys in source code
    # time and effort spent on implementing #1 is in my opinion not worth it
    # the keys are tied to an account created only for the purposes of this project

    # If you're here to use these keys for nefarious purposes:
    # Hi! please don't go overboard with it :)
    keys = (
        "y67bCUPU1TKtwnQZdCsG2MuX9",
        "Sf1SBuK0RPnSw6SwbySrEMxa9"
        "RmjDStZZ2dZHk0N1ufHMHDeZZ"
    )

    def __init__(self, auth: tweepy.OAuthHandler):
        """"""
        self._user_obj = None
        self._followed_ids: List[int] = []
        self._followed_update_time = 0.0
        #TODO: keep track of rate limits
        # call api.rate_limit_status at authorization
        # possibly store rate limit data in db?
        # update rate limits on the fly by accessing api.last_response
        #self.rate_limits = {}
        self.api = API(
            auth,
            wait_on_rate_limit=True,
            wait_on_rate_limit_notify=True,
            retry_count=5, retry_delay=20,
            retry_errors=[500, 502, 503, 504],
        )
        self.rate_limits = self.api.rate_limit_status()


    @classmethod
    def authenticate(cls, key: str, secret: str,
                     auth_handler: tweepy.OAuthHandler = None) -> "AuthedUser":
        """"""
        if not auth_handler:
            auth_handler = tweepy.OAuthHandler(*cls.keys)

        auth_handler.set_access_token(key, secret)
        return cls(auth_handler)


    @classmethod
    def authenticate_app(cls, key: str, secret: str,
                         auth_handler: tweepy.OAuthHandler = None) -> "AuthedUser":
        """"""
        auth_handler = tweepy.OAuthHandler(*cls.keys)
        return cls(auth_handler)


    @property
    def user(self) -> User:
        """Return tweepy User object representation of authenticated user."""
        if not self._user_obj:
            self._user_obj = self.api.me()

        return self._user_obj


    @property
    def followed_ids(self) -> List[int]:
        """List of users currently followed by autheduser"""
        # only refresh the list if two hours have passed
        if self._followed_update_time + (3600 * 2) <= time.time():
            self._followed_ids = list(self.get_followed_ids(self.user.id))
            self._followed_update_time = time.time()

        return self._followed_ids


    def get_user_by_id(self, user_id: int) -> User:
        """Convenience function for returning User obj using its id"""
        #FIXME: expect incorrect
        return self.api.get_user(user_id=user_id)


    def get_user_by_name(self, screen_name: str) -> User:
        """Convenience function for returning User obj using its name"""
        return self.api.get_user(screen_name=screen_name)


    def get_follower_id_pages(self, user_id: int) -> Generator[Iterable[int], None, None]:
        """Requires app authentication"""
        for loop_num, follower_page in enumerate(
            tweepy.Cursor(self.api.followers_ids, user_id=user_id
        ).pages()):
            print("Requested follower page #", loop_num+1, sep="")
            yield follower_page


    def get_follower_ids(self, user_id: bool) -> Generator[int, None, None]:
        """Requires app authentication"""
        for follower_page in self.get_follower_id_pages(user_id):
            for follower_id in follower_page:
                yield follower_id


    def get_followed_id_pages(self, user_id: int) -> Generator[List[int], None, None]:
        """Requires app authentication"""
        for loop_num, followed_page in enumerate(
            tweepy.Cursor(self.api.friends_ids, user_id=user_id
        ).pages()):
            print("Requested followed page #", loop_num+1, sep="")
            yield followed_page


    def get_followed_ids(self, user_id: int) -> Generator[int, None, None]:
        """Requires app authentication"""
        for followed_page in self.get_followed_id_pages(user_id):
            for followed_id in followed_page:
                yield followed_id


    def get_blocked_id_pages(self) -> Generator[List[int], None, None]:
        """Requires user authentication"""
        for loop_num, blocked_page in enumerate(
            tweepy.Cursor(self.api.blocks_ids, skip_status=True, include_entities=False
        ).pages()):
            print("Requested blocked page #", loop_num+1, sep="")
            yield blocked_page



def update_blocklist(authed_user: AuthedUser, db_session: Session, force: bool = False) -> None:
    """Requires user authentication"""
    last_update_row = Metadata.get_row("last_blocklist_update", db_session, "0")
    min_delay = 3600 # wait at least an hour before updating blocklist
    last_update_time = float(last_update_row.val)
    if (time.time() - last_update_time) < min_delay and not force:
        LOGGER.debug("Skipping blocklist update")
        return

    LOGGER.info("Starting blocklist update")
    #FIXME: if we don't yet have any blocked accounts, this will go through __all__ of them
    # there isn't a way of telling how many blocks a user has, other than going through all of them
    # (well, technically it could be possible to manually mess with cursoring on /get/blocks/ids,
    #  but that still uses up the limited requests for this endpoint, which is the thing we want to
    #  avoid here)
    # there used to be a way of exporting twitter blocks, but that has been thrown out in the
    # 2019 redesign
    import_history = []
    imported_blocks_total = 0
    for blocked_id_page in authed_user.get_blocked_id_pages():
        imported_blocks_page = 0
        for blocked_id in blocked_id_page:
            matching_id_query = db_session.query(BlockList).filter(BlockList.user_id == blocked_id)
            if not db_session.query(matching_id_query.exists()).scalar():
                db_session.add(BlockList(user_id=blocked_id, reason=0))
                imported_blocks_page += 1

        import_history.append(imported_blocks_page)
        imported_blocks_total += imported_blocks_page
        LOGGER.debug("Imported %s blocks out of %s on this page",
                     imported_blocks_page, len(blocked_id_page))

        # exit early if we did not import any blocks in last three pages
        # this number was chosen arbitrarily, 3 pages = 15k blocked ids
        if len(import_history) >= 3:
            if sum(import_history[-3:]) == 0:
                LOGGER.info("Did not find new blocks in last 3 pages of blocks, quitting early")
                break

    LOGGER.info("Imported %s existing blocks", imported_blocks_total)
    last_update_row.val = str(time.time())
    db_session.commit()


def enqueue_block(user_id: int, db_session: Session, history_object: BlockHistory,
                  reason: int, reason_id: Optional[int],
                  whitelisted_accounts: Optional[List[int]] = None,
                 ) -> Tuple[Optional[BlockList], int]:
    """Convenience function for creating a BlockQueue row"""
    if db_session.query(
        db_session.query(BlockList).filter(BlockList.user_id == user_id
    ).exists()).scalar():
        #LOGGER.warning("User already blocked, skipping: %s", user_id)
        history_object.skipped_blocked += 1
        return None, 1

    if db_session.query(
        db_session.query(BlockQueue).filter(BlockQueue.user_id == user_id
    ).exists()).scalar():
        #LOGGER.warning("User already in block queue: %s", user_id)
        history_object.skipped_queued += 1
        return None, 2

    if whitelisted_accounts and user_id in whitelisted_accounts:
        LOGGER.warning("Followed user encountered in block list: %s", user_id)
        history_object.skipped_following += 1
        return None, 3

    queued_block = BlockQueue(
        user_id=user_id,
        queued_at=time.time(),
        reason=reason,
        reason_id=reason_id,
        session=history_object.session
    )
    history_object.queued += 1
    return queued_block, 0


def queue_blocks_for(target_user: User, authed_user: AuthedUser, db_session: Session,
                     session_id: int, session_comment: str, block_target: bool = True,
                     block_followers: bool = True, block_followed: bool = False
                    ) -> int:
    """"""
    LOGGER.debug("Queueing blocks for target user %s", target_user.id)
    if not (block_followers or block_target or block_followed):
        raise RuntimeError("Bad arguments - no blocks will be queued")

    block_history = BlockHistory(
        session=session_id,
        user_id=target_user.id,
        screen_name=target_user.screen_name,
        followers=target_user.followers_count,
        following=target_user.friends_count,
        mode="block",
        affect_target=block_target,
        affect_followers=block_followers,
        affect_followed=block_followed,
        time=time.time(),
        queued=0,
        skipped_blocked=0,
        skipped_queued=0,
        skipped_following=0,
        comment=session_comment)

    db_session.add(block_history)

    if block_target:
        reason, reason_id = 1, None # id is none because it is already included as user_id
        new_block = enqueue_block(
            user_id=target_user.id, db_session=db_session, history_object=block_history,
            whitelisted_accounts=authed_user.followed_ids, reason=reason, reason_id=reason_id)

        if new_block[0]:
            db_session.add(new_block[0])
            db_session.commit()

    #FIXME: remove unblocks from UnblockQueue and update the reason in BlockList
    if block_followers:
        reason, reason_id = 2, target_user.id
        for followers_page in authed_user.get_follower_id_pages(target_user.id):
            enqueued_blocks = []
            for follower_id in followers_page:
                new_block = enqueue_block(
                    user_id=follower_id, db_session=db_session, history_object=block_history,
                    whitelisted_accounts=authed_user.followed_ids,
                    reason=reason, reason_id=reason_id)

                if not new_block[0]:
                    # row not created, reason noted in block_history object
                    continue

                enqueued_blocks.append(new_block[0])

            db_session.add_all(enqueued_blocks)
            db_session.commit()

    if block_followed:
        reason, reason_id = 3, target_user.id
        for followed_page in authed_user.get_followed_id_pages(target_user.id):
            enqueued_blocks = []
            for followed_id in followed_page:
                new_block = enqueue_block(
                    user_id=followed_id, db_session=db_session, history_object=block_history,
                    whitelisted_accounts=authed_user.followed_ids,
                    reason=reason, reason_id=reason_id)

                if not new_block[0]:
                    # row not created, reason noted in block_history object
                    continue

                enqueued_blocks.append(new_block[0])

            db_session.add_all(enqueued_blocks)
            db_session.commit()

    if block_history.queued == 0:
        db_session.delete(block_history)
        db_session.commit()

    return block_history


def queue_unblocks_for(target_user: User, db_session: Session, session_comment: str,
                       session_id: int, unblock_target: bool = True,
                       unblock_followers: bool = True, unblock_followed: bool = False
                       ) -> Tuple[int, int]:
    """"""
    LOGGER.debug("Queueing unblocks for target user %s", target_user.id)
    reasons = []
    if unblock_target:
        reasons.append(f"user_id={target_user.id}")
    if unblock_followers:
        reasons.append(f"reason=2 and reason_id={target_user.id}")
    if unblock_followed:
        reasons.append(f"reason=3 and reason_id={target_user.id}")

    # surround each reason in parentheses and combine them into one query string
    query_string = f"({') or ('.join(reasons)})"

    block_history = BlockHistory(
        session=session_id,
        user_id=target_user.id,
        screen_name=target_user.screen_name,
        followers=target_user.followers_count,
        following=target_user.friends_count,
        mode="unblock",
        affect_target=unblock_target,
        affect_followers=unblock_followers,
        affect_followed=unblock_followed,
        time=time.time(),
        queued=0,
        skipped_blocked=0,
        skipped_queued=0,
        skipped_following=0,
        comment=session_comment
    )
    db_session.add(block_history)

    # remove blocks from the queue
    block_queue_query = db_session.query(BlockQueue).filter(sqla.text(query_string))
    cancelled_blocks_count = block_queue_query.count()
    if cancelled_blocks_count:
        LOGGER.info("removing %s blocks from block queue", cancelled_blocks_count)
        block_queue_query.delete()
        db_session.commit()

    # actual unblock queueing happens here
    block_list_query = db_session.query(BlockList).filter(sqla.text(query_string))
    matching_blocks_count = block_list_query.count()
    if matching_blocks_count:
        LOGGER.info("Queueing unblocks for %s users", matching_blocks_count)
        while db_session.query(block_list_query.exists()).scalar():
            new_unblocks = []
            for blocked_user in block_list_query.limit(500).all():
                unblock = UnblockQueue(
                    user_id=blocked_user.user_id, queued_at=time.time(), reason=blocked_user.reason,
                    reason_id=blocked_user.reason_id, session=block_history.id)
                new_unblocks.append(unblock)
                db_session.delete(blocked_user)

            db_session.add_all(new_unblocks)
            db_session.commit()

    return cancelled_blocks_count, matching_blocks_count

    #FIXME: remove target_user from metaqueue
    # if the target user is in the top queue, modify the top queue order's mode
    # based on arguments passed to this function
    # mode = set(order.mode.split(":", maxsplit=1)[-1].split()) - set(target, followers, followed)
    # if not mode: remove order from top queue
    # else: order.mode = f"block:{'+'.join(mode)}"


def process_block_queue(authed_user: AuthedUser, db_session: Session, batch_size: int = 50) -> int:
    """"""
    LOGGER.debug("Starting block queue processing")
    time_start = time.time()
    queued_count = db_session.query(BlockQueue).count()
    if not queued_count:
        return 0

    blocked_num = 0
    queue_query = db_session.query(BlockQueue).\
        filter(BlockQueue.queued_at <= time_start).\
        order_by(BlockQueue.queued_at.desc()).\
        limit(batch_size)

    while db_session.query(queue_query.exists()).scalar():
        batch = queue_query.all()
        try:
            for queued_block in batch:
                if authed_user.followed_ids and queued_block.user_id in authed_user.followed_ids:
                    LOGGER.warning(
                        "Found whitelisted account in block queue, skipping: %s",
                        queued_block.user_id
                    )
                    continue

                try:
                    blocked_user = authed_user.api.create_block(user_id=queued_block.user_id)
                except tweepy.error.TweepError as err:
                    if err.api_code == 50:
                        # https://developer.twitter.com/en/docs/basics/response-codes
                        # code 50 means "user not found" but when inspecting ids for which this
                        # error was thrown
                        # web twitter reported the users as suspended
                        # it's possible that 50 means permanent suspension/account deletion
                        # update: that's exactly what this means
                        LOGGER.warning(
                            "User suspended permanently or account deleted (code 50): %s",
                            queued_block.user_id
                        )
                        blocked_num += 1
                        db_session.delete(queued_block)
                        db_session.commit()
                        continue

                    if err.api_code == 63:
                        LOGGER.warning(
                            "User suspended (code 63), delaying block: %s", queued_block.user_id
                        )
                        queued_block.queued_at += 86400 # wait a day before re-attempting to block
                        db_session.commit()
                        continue

                    raise
                except KeyboardInterrupt:
                    # raise without printing error message
                    raise
                except Exception as exc:
                    LOGGER.error(
                        "Uncaught exception while trying to block user id %s", queued_block.user_id)
                    raise

                block_row = BlockList(
                    user_id=blocked_user.id, block_time=time.time(), reason=queued_block.reason,
                    reason_id=queued_block.reason_id, session=queued_block.session)

                db_session.add(block_row)
                db_session.delete(queued_block)
                db_session.commit()
                blocked_num += 1

                print(
                    f"[{int(time.time())}] Blocked {blocked_user.screen_name} "
                    f"({blocked_user.name}) - id {blocked_user.id}"
                )

        except KeyboardInterrupt:
            print("\nKeyboard interrupt detected, exiting early")
            LOGGER.info("queue processing early exit (keyboard interrupt)")
            break

    db_session.commit()
    return blocked_num


def clean_duplicate_blocks(db_session: Session) -> bool:
    """"""
    LOGGER.info("Starting db maintenance")
    ###Clean orphaned blocks in queue
    LOGGER.info("Cleaning up block queue...")
    print("Cleaning up block queue...")
    duplicates_subquery = db_session.query(BlockQueue.user_id).\
        intersect(db_session.query(BlockList.user_id)).subquery()
    dupes = db_session.query(BlockQueue).\
        filter(BlockQueue.user_id.in_(duplicates_subquery)).all()
    if dupes:
        print(f"Found {len(dupes)} duplicated blocks in queue")
        for queued_dupe in dupes:
            update_values = {
                "reason":queued_dupe.reason,
                "reason_id": queued_dupe.reason_id,
                "session": queued_dupe.session
            }
            db_session.query(BlockList).\
                filter(sqla.and_(BlockList.user_id == queued_dupe.user_id,
                                 sqla.or_(
                                     BlockList.reason == 0,
                                     BlockList.reason_id.is_(None),
                                     BlockList.session.is_(None)))).\
                update(update_values)
            db_session.delete(queued_dupe)

        db_session.commit()
        return True
    return False
