""""""
import time
import shutil
import logging
import datetime
from pathlib import Path

from argparse import ArgumentParser
from typing import Any, Generator, Iterable, List, Optional, Tuple

import tweepy
from tweepy.models import User

import sqlalchemy as sqla
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from . import config

DeclarativeBase = declarative_base()

WORKING_DIR = Path.home() / "Twitter ChainBlocker"
WORKING_DIR.mkdir(exist_ok=True)

LOG_FORMAT_TERM = logging.Formatter("[%(levelname)s] %(message)s")
LOGGER = logging.getLogger("ChainBlocker")
LOGGER.setLevel(logging.DEBUG)
TH = logging.StreamHandler()
TH.setLevel(logging.WARNING)
TH.setFormatter(LOG_FORMAT_TERM)

LOGGER.addHandler(TH)

AUTH = tweepy.OAuthHandler(config.TWITTER_CONSUMER_API_KEY, config.TWITTER_SECRET_API_KEY)
AUTH.set_access_token(config.TWITTER_ACCESS_TOKEN, config.TWITTER_SECRET_TOKEN)
TW_API = tweepy.API(AUTH,
                    wait_on_rate_limit=True,
                    wait_on_rate_limit_notify=True,
                    retry_count=5, retry_delay=60,
                    retry_errors=[500, 502, 503, 504]
                   )


class Metadata(DeclarativeBase):
    """"""
    __tablename__ = "metadata"
    key = sqla.Column(sqla.String, primary_key=True)
    val = sqla.Column(sqla.String)

    @classmethod
    def get_row(cls, key_name: str, db_session: Session, default_val="") -> "Metadata":
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
        Creates the row if it does not exist.
        """
        row = cls.get_row(key_name, db_session)
        row.val = str(value)
        db_session.commit()
        return row


class BlockHistory(DeclarativeBase):
    """"""
    __tablename__ = "history"
    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.Integer)
    screen_name = sqla.Column(sqla.String)
    followers = sqla.Column(sqla.Integer)
    following = sqla.Column(sqla.Integer)
    mode = sqla.Column(sqla.String) # "block/unblock:followers+target+following"
    time = sqla.Column(sqla.Float)
    queued = sqla.Column(sqla.Integer)
    skipped_blocked = sqla.Column(sqla.Integer)
    skipped_queued = sqla.Column(sqla.Integer)
    skipped_following = sqla.Column(sqla.Integer)
    session_comment = sqla.Column(sqla.String)
    #FIXME: implement comment (one comment for session's batch of accounts)


class BlockList(DeclarativeBase):
    """"""
    __tablename__ = "blocked_accounts"
    user_id = sqla.Column(sqla.Integer, primary_key=True)
    screen_name = sqla.Column(sqla.String)
    block_time = sqla.Column(sqla.Float)
    reason = sqla.Column(sqla.String)


class BlockQueue(DeclarativeBase):
    """"""
    __tablename__ = "block_queue"
    user_id = sqla.Column(sqla.Integer, primary_key=True)
    queued_at = sqla.Column(sqla.Float)
    #group = sqla.Column(sqla.String)
    #FIXME: info on whether this is a follower / followed / target for unblock functionality
    reason = sqla.Column(sqla.String)


class UnblockQueue(DeclarativeBase):
    """"""
    __tablename__ = "unblock_queue"
    user_id = sqla.Column(sqla.Integer, primary_key=True)



class MetaQueue(DeclarativeBase):
    """"""
    id = sqla.Column(sqla.Integer, primary_key=True)
    user_id = sqla.Column(sqla.Integer)
    screen_name = sqla.Column(sqla.String)
    followers = sqla.Column(sqla.Integer)
    following = sqla.Column(sqla.Integer)
    action = sqla.Column(sqla.String)
    session_comment = sqla.Column(sqla.String)

#FIXME: finish implementing metaqueue

def get_user(user_id: Optional[int] = None,
             screen_name: Optional[str] = None) -> User:
    """"""
    if not (user_id or screen_name):
        raise ValueError("Either user id or screen name must be provided")
    if user_id and not isinstance(user_id, int):
        raise TypeError("User id must be an integer")
    if screen_name and not isinstance(screen_name, str):
        raise TypeError("Screen name must be a string")

    if user_id:
        return TW_API.get_user(user_id=user_id)

    return TW_API.get_user(screen_name=screen_name)


def get_follower_id_pages(user_id: int) -> Generator[Iterable[int], None, None]:
    """"""
    for loop_num, follower_page in enumerate(tweepy.Cursor(TW_API.followers_ids, user_id=user_id).pages()):
        print("Requested follower page #", loop_num+1, sep="")
        yield follower_page


def get_follower_ids(user_id: bool) -> Generator[int, None, None]:
    """"""
    for follower_page in get_follower_id_pages(user_id):
        for follower_id in follower_page:
            yield follower_id


def get_followed_id_pages(user_id: int) -> Generator[List[int], None, None]:
    """"""
    for loop_num, followed_page in enumerate(tweepy.Cursor(TW_API.friends_ids, user_id=user_id).pages()):
        print("Requested followed page #", loop_num+1, sep="")
        yield followed_page


def get_followed_ids(user_id: int) -> Generator[int, None, None]:
    """"""
    for followed_page in get_followed_id_pages(user_id):
        for followed_id in followed_page:
            yield followed_id


def get_blocked_ids() -> Generator[int, None, None]:
    """"""
    for loop_num, blocked_page in enumerate(tweepy.Cursor(TW_API.blocks_ids, skip_status=True, include_entities=False).pages()):
        print("Requested blocked page #", loop_num+1, sep="")
        for blocked in blocked_page:
            yield blocked


def update_blocklist(db_session: Session, force: bool = False) -> None:
    """"""
    last_update_row = Metadata.get_row("last_blocklist_update", db_session, "0")
    # only update blocklist if at least a day has passed since last update
    min_delay = 86400
    last_update_time = float(last_update_row.val)
    if (time.time() - last_update_time) < min_delay:
        return

    print("Updating account's blocklist, this might take a while...")
    for blocked_id in get_blocked_ids():
        matching_id_query = db_session.query(BlockList).filter(BlockList.user_id == blocked_id)
        if not db_session.query(matching_id_query.exists()).scalar():
            db_session.add(BlockList(user_id=blocked_id, reason="unknown"))

    last_update_row.val = str(time.time())
    db_session.commit()


def enqueue_block(user_id: int, block_reason: str, db_session: Session, history_object: BlockHistory,
                  whitelisted_accounts: Optional[List[int]] = None) -> Tuple[Optional[BlockList], int]:
    """Convenience function for creating a BlockQueue row"""
    if db_session.query(db_session.query(BlockList).filter(BlockList.user_id == user_id).exists()).scalar():
        #LOGGER.warning("User already blocked, skipping: %s", user_id)
        history_object.skipped_blocked += 1
        return None, 1

    if db_session.query(db_session.query(BlockQueue).filter(BlockQueue.user_id == user_id).exists()).scalar():
        #LOGGER.warning("User already in block queue: %s", user_id)
        history_object.skipped_queued += 1
        return None, 2

    if whitelisted_accounts and user_id in whitelisted_accounts:
        LOGGER.warning("Followed user encountered in block list: %s", user_id)
        history_object.skipped_following += 1
        return None, 3

    queued_block = BlockQueue(user_id=user_id, queued_at=time.time(), reason=str(block_reason))
    history_object.queued += 1
    return queued_block, 0


def block_followers_of(target_user: User, db_session: Session,
                       block_followers: bool = True, block_target: bool = True,
                       block_following: bool = False,
                       whitelisted_accounts: Optional[List[int]] = None) -> int:
    """"""
    if not (block_followers or block_target or block_following):
        LOGGER.warning("Bad arguments - no blocks will be queued")
        return 0

    print(target_user.screen_name, ": This user has", target_user.followers_count, "followers")
    block_reason = str(target_user.id)

    mode_str = []
    if block_followers:
        mode_str.append("followers")
    if block_target:
        mode_str.append("target")
    if block_following:
        mode_str.append("following")

    time_start = time.time()

    block_history = BlockHistory(
        user_id=target_user.id, screen_name=target_user.screen_name,
        followers=target_user.followers_count, following=target_user.friends_count, mode="+".join(mode_str),
        time=time_start, queued=0, skipped_blocked=0, skipped_queued=0, skipped_following=0)

    db_session.add(block_history)
    del mode_str

    if block_followers:
        for followers_page in get_follower_id_pages(target_user.id):
            enqueued_blocks = []
            for follower_id in followers_page:
                new_block = enqueue_block(
                    follower_id, block_reason, db_session, history_object=block_history,
                    whitelisted_accounts=whitelisted_accounts)

                if not new_block[0]:
                    # row not created, reason noted in block_history object
                    continue

                enqueued_blocks.append(new_block[0])

            db_session.add_all(enqueued_blocks)
            db_session.commit()

    if block_following:
        raise NotImplementedError()
        for followed_page in get_followed_id_pages(target_user.id):
            enqueued_blocks = []
            for followed_id in followed_page:
                new_block = enqueue_block(
                    followed_id, block_reason, db_session, history_object=block_history,
                    whitelisted_accounts=whitelisted_accounts)

                if not new_block[0]:
                    # row not created, reason noted in block_history object
                    continue

                enqueued_blocks.append(new_block[0])

            db_session.add_all(enqueued_blocks)
            db_session.commit()

    if block_target:
        new_block = enqueue_block(
            target_user.id, block_reason, db_session, history_object=block_history,
            whitelisted_accounts=whitelisted_accounts)

        if new_block[0]:
            db_session.add(new_block[0])
            db_session.commit()

    if block_history.queued == 0:
        db_session.delete(block_history)
        db_session.commit()

    time_total = time.time() - time_start
    print(f"Queued:          {block_history.queued}")
    print(f"Already queued:  {block_history.skipped_queued}")
    print(f"Already blocked: {block_history.skipped_blocked}")
    print(f"Following:       {block_history.skipped_following}")
    time_str = f"{int(time_total // 3600)}h {int((time_total / 60) % 60)}m {int(time_total % 60)}s"
    print(f"This took {time_str}")
    LOGGER.info("Stats: queued=%s, skipped_blocked=%s, skipped_queued=%s, skipped_following=%s, time=%s",
                block_history.queued, block_history.skipped_blocked, block_history.skipped_queued,
                block_history.skipped_following, time_str)

    return block_history.queued


def unblock_followers_of(target_user: User, db_session: Session,
                         unblock_followers: bool = True, unblock_target: bool = True,
                         unblock_following: bool = False) -> int:
    """"""
    #FIXME: this is essentially the last bit of the basic functionality that's missing
    raise NotImplementedError


def process_block_queue(db_session: Session, whitelisted_accounts: Optional[List[int]] = None, batch_size: int = 20) -> int:
    """"""
    time_start = time.time()
    queued_count = db_session.query(BlockQueue).count()
    if not queued_count:
        print("Block queue empty")
        return 0

    print(f"There are {queued_count} accounts in the queue")
    LOGGER.info("There are %s accounts in the queue", queued_count)
    time_str = str(datetime.timedelta(seconds=queued_count))
    print(f"Which should take {time_str} (at 1 second per request)")
    LOGGER.info("Which should take %s (at 1 second per request)", time_str)

    blocked_num = 0
    queue_query = db_session.query(BlockQueue).filter(BlockQueue.queued_at <= time_start).order_by(BlockQueue.queued_at.desc())
    while db_session.query(queue_query.exists()).scalar():
        batch = queue_query.limit(batch_size).all()
        try:
            for queued_block in batch:
                if whitelisted_accounts and queued_block.user_id in whitelisted_accounts:
                    print("Found whitelisted account in block queue, skipping")
                    continue

                try:
                    blocked_user = TW_API.create_block(user_id=queued_block.user_id)
                except tweepy.error.TweepError as err:
                    if err.api_code == 50:
                        # https://developer.twitter.com/en/docs/basics/response-codes
                        # code 50 means "user not found" but when inspecting ids for which this error was thrown
                        # web twitter reported the users as suspended
                        # it's possible that 50 means permanent suspension/account deletion
                        # update: that's exactly what this means
                        LOGGER.warning("User suspended permanently or account deleted (code 50): %s", queued_block.user_id)
                        blocked_num += 1
                        db_session.delete(queued_block)
                        db_session.flush()
                        continue

                    if err.api_code == 63:
                        LOGGER.warning("User suspended (code 63), delaying block: %s", queued_block.user_id)
                        queued_block.queued_at += 86400 # wait a day before before re-attempting to block
                        db_session.flush()
                        continue

                    #tweepy.error.TweepError: Failed to send request: ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))
                    # ^ err.api_code and err.response are None
                    #FIXME: handle network errors by exiting early
                    raise
                except:
                    LOGGER.error("Uncaught exception while trying to block user id %s", queued_block.user_id)
                    raise

                block_row = BlockList(
                    user_id=blocked_user.id, screen_name=blocked_user.screen_name,
                    block_time=time.time(), reason=queued_block.reason)

                print(f"Blocked {blocked_user.screen_name} ({blocked_user.name}) - id {blocked_user.id}")

                db_session.add(block_row)
                db_session.delete(queued_block)
                db_session.commit()
                blocked_num += 1
        except KeyboardInterrupt:
            print("\nKeyboard interrupt detected, exiting early")
            LOGGER.info("queue processing early exit (keyboard interrupt)")
            break

    db_session.commit()

    time_total = time.time() - time_start
    time_str = str(datetime.timedelta(seconds=time_total))
    print(f"Processed {blocked_num} out of {queued_count} blocks ({blocked_num / queued_count * 100:.2f}%)")
    LOGGER.info("Processed %s out of %s blocks)", blocked_num, queued_count)
    print(f"This took {time_str}")
    LOGGER.info("Processing took %s", time_str)
    LOGGER.info("processing + networking per block = %ss avg", blocked_num / time_total)

    return blocked_num


def db_maintenance(db_session: Session) -> None:
    ###Clean orphaned blocks in queue
    last_user_id = 0
    block_queue_query = db_session.query(BlockQueue).filter(BlockQueue.user_id > last_user_id).order_by(BlockQueue.user_id)
    LOGGER.info("Cleaning up block queue...")
    while db_session.query(block_queue_query.exists()).scalar():
        for queued_block in block_queue_query.limit(1000).all():
            last_user_id = queued_block.user_id
            matching_id_query = db_session.query(BlockList).filter(BlockList.user_id == queued_block.user_id)
            if db_session.query(matching_id_query.exists()).scalar():
                # remove block from queue if it has already been blocked
                # this can happen after blocklist update or early exit in process_queue
                LOGGER.warning("Deleting already blocked user from queue: %s", queued_block.user_id)
                db_session.delete(queued_block)

        db_session.commit()
        block_queue_query = db_session.query(BlockQueue).filter(BlockQueue.user_id > last_user_id).order_by(BlockQueue.user_id)

    #TODO: ^ do the same for unblocks
    ###Clean orphaned unblocks in queue

    ###Vacuum the database
    vacuum_delay = 86400
    last_vacuum_row = Metadata.get_row("last_vacuum", db_session, "0")
    if float(last_vacuum_row.val) + vacuum_delay <= time.time():
        LOGGER.info("Vacuuming database...")
        #TODO: perform db vacuum
        #last_vacuum_row.val = str(time.time())

    db_session.commit()


ARGPARSER = ArgumentParser(prog="chainblocker")
ARGPARSER.add_argument("account_name", default=[], nargs="*", help="")
ARGPARSER.add_argument("--skip-blocklist-update", action="store_true",
                       help="")
ARGPARSER.add_argument("--only-queue-blocks", action="store_true",
                       help="Accounts will be queued for blocking, but they won't be blocked until blocker is ran without this option enabled")
ARGPARSER.add_argument("--dont-block-target", action="store_true",
                       help="Do not block target accounts, only their followers")
ARGPARSER.add_argument("--dont-block-followers", action="store_true",
                       help="")
ARGPARSER.add_argument("--block-targets-followed", action="store_true",
                       help="Block accounts followed by target account")
#TODO: implement show_user_info -just pretty print the User object + number of blocked users + block reason
#TODO: implement session comments, with the default comment being the time of the session'start
#TODO:
def main(args: Optional[str] = None) -> None:
    """"""
    args = ARGPARSER.parse_args(args)

    authenticated_user = TW_API.me()

    dbfile = WORKING_DIR / f"{authenticated_user.id}_blocklist.sqlite"
    sqla_engine = sqla.create_engine(f"sqlite:///{str(dbfile)}", echo=False)
    DeclarativeBase.metadata.create_all(sqla_engine)
    bound_session = sessionmaker(bind=sqla_engine)

    LOGGER.info("Creating new db session")
    session_start = time.time()
    db_session = bound_session()
    blocks_queued = 0
    try:
        clean_exit = Metadata.get_row("clean_exit", db_session, "1")
        if clean_exit.val == "0":
            LOGGER.warning("Exception encountered in last session, performing maintenance")
            db_maintenance(db_session)

        print("getting authenticated user's follows...")
        authenticated_user_follows = [x for x in get_followed_ids(authenticated_user.id)]
        if not args.skip_blocklist_update:
            update_blocklist(db_session)

        if args.account_name:
            #if not args.comment:
                #comment = f"Instance started at {datetime.datetime.now.isoformat()}"
            for account_name in args.account_name:
                target_user = get_user(screen_name=account_name)
                LOGGER.info("Queueing blocks for followers of USER=%s ID=%s", target_user.screen_name, target_user.id)
                blocks_queued += block_followers_of(
                    target_user, db_session,
                    block_followers=(not args.dont_block_followers), block_target=(not args.dont_block_target),
                    block_following=(args.block_targets_followed), whitelisted_accounts=authenticated_user_follows)

        print(f"Added {blocks_queued} new accounts to block queue")

        if not args.only_queue_blocks:
            print("Processing block queue")
            process_block_queue(db_session, authenticated_user_follows)
        Metadata.set_row("clean_exit", 1, db_session)
    except:
        LOGGER.error("Uncaught exception, rolling back db session")
        db_session.rollback()
        Metadata.set_row("clean_exit", 0, db_session)
        raise
    finally:
        LOGGER.info("Closing db session")
        db_session.close()


if __name__ == "__main__":
    FH = logging.FileHandler(WORKING_DIR / "chainblocker.log", mode="w")
    FH.setLevel(logging.INFO)
    FH.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s: %(message)s"))
    LOGGER.addHandler(FH)
    try:
        main()
    except Exception as exc:
        # ignore argparse-issued systemexit
        if not isinstance(exc, SystemExit):
            LOGGER.exception("UNCAUGHT EXCEPTION:")
            exception_log = WORKING_DIR / time.strftime("chainblocker_exception_%Y-%m-%dT_%H-%M-%S.log")
            shutil.copy(FH.baseFilename, exception_log)
            print("Chain blocker quit due to unexpected error!")
            print(f"Error: {exc}")
            print(f"Traceback has been saved to {str(exception_log)}")
            print("If this issue persists, please report it to the project's github repo: https://github.com/rmmbear/twitter-chain-blocker")
