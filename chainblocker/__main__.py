""""""
import os
import sys
import time
import shutil
import logging
import datetime
from typing import Optional

from pathlib import Path
from argparse import ArgumentParser

import tweepy
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker, Session

import chainblocker

LOGGER = logging.getLogger(__name__)

ARGPARSER = ArgumentParser(
    prog="chainblocker",
    description="All account arguments must be passed in form of screen names (aka 'handles'), "
                "and not display names or IDs. Screen names are resolved to IDs internally, which "
                "means that this program will work even when blocked users change their account names. "
                "If, for any reason, chainblocker was stopped while processing queues and you would "
                "like to resume without adding anything to the queue, simply run it again without a command")
### Top-level arguments
ARGPARSER.add_argument(
    "--skip-blocklist-update", action="store_true",
    help="Do not update account's blocklist before queueing/processing.")
ARGPARSER.add_argument(
    "--only-queue-accounts", action="store_true",
    help="Delay queue processing until next run and only queue accounts for blocking/unblocking.")
ARGPARSER.add_argument(
    "--only-queue-actions", action="store_true",
    help="Delay queueing accounts until next run and only store which actions to perform. "
         "Useful if you want to issue different commands one after another, and don't want to wait for account queueing. "
         "This option also disables blocklist update")
ARGPARSER.add_argument(
    "--mode", type=str, default="target+followers",
    help="Set which parties will be affected in current batch of accounts. "
         "Options must be delimited with a '+' symbol. Possible options are: "
         "target = the account named, followers = target's followers, followed = people followed by target. "
         "Mode defaults to target+followers for both blocking and unblocking")
ARGPARSER.add_argument(
    "--comment", type=str,
    help="Set the comment for this batch operation. This comment will be displayed when querying block reason. "
         "If left empty, comment will be automatically set to "
         "'Session {year}/{month}/{day} {hours}:{minutes}:{seconds}, queried {number} accounts'")

ARGP_COMMANDS = ARGPARSER.add_subparsers(title="Commands", dest="command", metavar="")
### Block command
ARGP_BLOCK = ARGP_COMMANDS.add_parser(
    "block",
    help="Block specified accounts and their followers (use --mode to change this behavior)")
ARGP_BLOCK.add_argument(
    "accounts", nargs="*",
    help="List of screen names of accounts you wish to block")

### Unblock command
ARGP_UNBLOCK = ARGP_COMMANDS.add_parser(
    "unblock",
    help="Unblock (or remove from the block queue) specified accounts and their followers "
         "(use --mode to change this behavior, for example: '--mode followers' to only unblock "
         "followers of the target account, but keep the target account blocked")
ARGP_UNBLOCK.add_argument(
    "accounts", nargs="*",
    help="List of screen names of accounts you wish to unblock.")

### Reason command
ARGP_REASON = ARGP_COMMANDS.add_parser("reason", help="Check if you are blocking someone and display details of that block")
ARGP_REASON.add_argument(
    "account_name", type=str,
    help="Screen name of the account you want to query")

#TODO: implement session comments, with the default comment being the time of the session's start

def get_workdirs() -> dict:
    """"""
    paths = {}
    dirname = "Twitter Chainblocker"
    home = Path.home()
    # do not clutter up people's home dir
    if os.name == "posix":
        paths["data"] = home / f".local/share/{dirname}"
        paths["config"] = home / f".config/{dirname}"
        return paths

    if os.name == "nt":
        home = Path(os.path.expandvars("%APPDATA%"))
        paths["data"] = home / f"Local/{dirname}/data"
        paths["config"] = home / f"Local/{dirname}/config"
        return paths

    paths["data"] = home / f"{dirname}/data"
    paths["config"] = home / f"{dirname}/config"
    return paths


def main(paths: dict, args: Optional[str] = None) -> None:
    """"""
    args = ARGPARSER.parse_args(args)
    print(args)
    args.mode = args.mode.split("+")
    if len(args.mode) > 3:
        sys.exit("ERROR: Received more than three targes for --mode\n"
                 "(only accepting 'target', 'followers' and 'followed')")

    unknown_mode = set(args.mode) - set(("target", "followers", "followed"))
    if unknown_mode:
        sys.exit(f"ERROR: {unknown_mode}: invalid --mode\n"
                 "(only accepting 'target', 'followers' and 'followed')")

    args.affect_target = "target" in args.mode
    args.affect_followers = "followers" in args.mode
    args.affect_followed = "followed" in args.mode

    #FIXME: implement all arguments
    NOT_IMPLEMENTED = ["only_queue_actions"]
    for missing in NOT_IMPLEMENTED:
        if getattr(args, missing, None):
            raise NotImplementedError(f"'{missing}' is not yet implemented")

    current_user = authenticate_interactive()

    dbfile = paths["data"] / f"{current_user.user.id}_blocklist.sqlite"
    sqla_engine = sqla.create_engine(f"sqlite:///{str(dbfile)}", echo=False)
    chainblocker.BlocklistDBBase.metadata.create_all(sqla_engine)
    bound_session = sessionmaker(bind=sqla_engine)

    LOGGER.info("Creating new db session")
    session_start = time.time()
    db_session = bound_session()

    if args.command in ("unblock", "block"):
        session_id = db_session.\
            query(sqla.sql.func.max(chainblocker.BlockHistory.session)).one_or_none()[0]
        if not session_id:
            session_id = 1
        else:
            session_id += 1

        if not args.comment:
            args.comment = time.strftime(f"Session %Y/%m/%d %H:%M:%S, queried {len(args.accounts)} accounts")

        #FIXME: expect errors when fetching users
        #https://developer.twitter.com/en/docs/basics/response-codes
        #args.accounts = [current_user.get_user(screen_name=user) for user in args.accounts]

    try:
        if chainblocker.Metadata.get_row("clean_exit", db_session, "1") == "0":
            LOGGER.warning("Exception encountered in last session, performing maintenance")
            print("Exception encountered in last session, performing maintenance")
            chainblocker.db_maintenance(db_session)

        if args.command == "reason":
            reason(target_user=args.account_name, authed_user=current_user, db_session=db_session)

        if args.command == "unblock":
            for unblock_target in args.accounts:
                unblock(
                    target_user=unblock_target,
                    authed_user=current_user,
                    db_session=db_session,
                    affect_target=args.affect_target,
                    affect_followers=args.affect_followers,
                    affect_followed=args.affect_followed,
                    session_comment=args.comment,
                    session_id=session_id
                )

        if args.command == "block":
            if not args.skip_blocklist_update and not args.only_queue_actions:
                print("Updating account's blocklist, this might take a while...")
                chainblocker.update_blocklist(current_user, db_session)
                print("Blocklist update complete\n")

            for block_target in args.accounts:
                block(
                    target_user=block_target,
                    authed_user=current_user,
                    db_session=db_session,
                    affect_target=args.affect_target,
                    affect_followers=args.affect_followers,
                    affect_followed=args.affect_followed,
                    session_comment=args.comment,
                    session_id=session_id
                )

        if not args.only_queue_accounts and not args.only_queue_actions and args.command != "reason":
            process_queues(current_user, db_session)

        chainblocker.Metadata.set_row("clean_exit", 1, db_session)
    # did you know that pylint does not report any errors from bare except blocks? I didn't
    except Exception as exc:
        LOGGER.error("Uncaught exception, rolling back db session")
        db_session.rollback()
        chainblocker.Metadata.set_row("clean_exit", 0, db_session)
        raise exc
    finally:
        LOGGER.info("Closing db session")
        db_session.close()


def authenticate_interactive() -> chainblocker.AuthedUser:
    """"""
    auth_handler = tweepy.OAuthHandler(*chainblocker.AuthedUser.keys)
    #TODO: implement key override - allow people to use their own keys for app-auth
    auth_url = auth_handler.get_authorization_url()
    print(f"Authnetication is required before we can continue.")
    print(f"Please go to the following url and authorize the app")
    print(f"{auth_url}")
    auth_pin = input("Please paste the PIN here: ").strip()
    #FIXME: perform error-checking, check input
    #FIXME: expect authentication errors
    access_token = auth_handler.get_access_token(auth_pin)
    auth_handler.set_access_token(*access_token)
    authed_user = chainblocker.AuthedUser(auth_handler)
    print(f"Authentication successful for user '{authed_user.user.screen_name}'\n")
    return authed_user


def reason(target_user: str, authed_user: chainblocker.AuthedUser, db_session: Session) -> None:
    """"""
    reason_string = \
        "User: {} (ID={})\n" \
        "Status: {}\n" \
        "Reason: {}\n" \
        #"Comment: {session_comment}\n"
    twitter_user = authed_user.get_user(screen_name=target_user)
    block_row = db_session.query(chainblocker.BlockList).filter(chainblocker.BlockList.user_id == twitter_user.id).one_or_none()
    if not block_row:
        reason_string = reason_string.format(
            twitter_user.screen_name, twitter_user.id, "Not in local block database!", "---")
    else:
        if block_row.reason == "target":
            reason_full = "First in chain (blocking target)"
        elif block_row.reason == "unknown":
            reason_full = "Unknown, this block was not made using chainblocker"
        else:
            #FIXME: expect errors retrieving users
            reason_user = authed_user.get_user(int(block_row.reason_id))
            reason_full = f"{block_row.reason.replace('_', ' ' )} {reason_user.screen_name} (ID={block_row.reason_id})"
        reason_string = reason_string.format(
            twitter_user.screen_name, twitter_user.id,
            time.strftime("Blocked on %Y/%m/%d %H:%M:%S", time.localtime(block_row.block_time)) if block_row.block_time else "Blocked on ???",
            reason_full)

    print(reason_string)


def block(target_user: str, authed_user: chainblocker.AuthedUser, db_session: Session,
          session_comment: str, session_id: int, affect_target: bool, affect_followers: bool,
          affect_followed: bool
         ) -> None:
    """"""
    target_user = authed_user.get_user(screen_name=target_user)
    print(target_user.screen_name, ": This user has", target_user.followers_count, "followers")
    LOGGER.info("Queueing blocks for followers of USER=%s ID=%s", target_user.screen_name, target_user.id)
    time_start = time.time()
    block_history = chainblocker.queue_blocks_for(
        target_user=target_user,
        authed_user=authed_user,
        db_session=db_session,
        block_target=affect_target,
        block_followers=affect_followers,
        block_followed=affect_followed,
        session_comment=session_comment,
        session_id=session_id
    )

    time_total = time.time() - time_start
    time_str = str(datetime.timedelta(seconds=time_total))
    print(f"Queued:          {block_history.queued}")
    print(f"Already queued:  {block_history.skipped_queued}")
    print(f"Already blocked: {block_history.skipped_blocked}")
    print(f"Following:       {block_history.skipped_following}")
    print(f"This took:       {time_str}")
    print()
    LOGGER.info(
        "Stats: queued=%s, skipped_blocked=%s, skipped_queued=%s, skipped_following=%s, time=%s",
        block_history.queued, block_history.skipped_blocked, block_history.skipped_queued,
        block_history.skipped_following, time_str
    )


def unblock(target_user: str, authed_user: chainblocker.AuthedUser, db_session: Session,
            session_comment: str, session_id: int, affect_target: bool, affect_followers: bool, affect_followed: bool
           ) -> None:
    """"""
    #FIXME: implement unblocking
    raise NotImplementedError()
    target_user = authed_user.get_user(screen_name=target_user)
    cancelled, queued = chainblocker.queue_unblocks_for(
        target_user,
        db_session,
        unblock_target=affect_target,
        unblock_followers=affect_followers,
        unblock_followed=affect_followed,
        session_comment=session_comment,
        session_id=session_id
    )

    print(f"Cancelled blocks: {cancelled}")
    print(f"Queued unblocks:  {queued}")
    print()


def process_queues(authed_user: chainblocker.AuthedUser, db_session: Session) -> None:
    """"""
    #FIXME: do not count blocks and unblocks "in the future"
    blocked_accs = db_session.query(chainblocker.BlockList).count()
    queued_blocks = db_session.query(chainblocker.BlockQueue).count()
    queued_unblocks = db_session.query(chainblocker.UnblockQueue).count()
    print("Current blocklist statistics:")
    print(f"Blocked accounts: {blocked_accs}")
    print(f"In Unblock Queue: {queued_unblocks}")
    print(f"In Block Queue:   {queued_blocks}")
    print()

    if queued_unblocks:
        print("Processing unblock queue")
        time_start = time.time()
        unblocked_num = chainblocker.process_block_queue(authed_user, db_session)
        time_total = time.time() - time_start

        time_str = str(datetime.timedelta(seconds=time_total))
        print(f"Processed {unblocked_num} out of {queued_unblocks} unblocks ({unblocked_num / queued_unblocks * 100:.2f}%)")
        print(f"This took {time_str}")
        print()
        LOGGER.info("Processed %s out of %s unblocks)", unblocked_num, queued_unblocks)
        LOGGER.info("Processing took %s", time_str)
        LOGGER.info("processing + networking per unblock = %ss avg", unblocked_num / time_total)

    if queued_blocks:
        print("Processing block queue")
        time_start = time.time()
        blocked_num = chainblocker.process_block_queue(authed_user, db_session)
        time_total = time.time() - time_start

        time_str = str(datetime.timedelta(seconds=time_total))
        print(f"Processed {blocked_num} out of {queued_blocks} blocks ({blocked_num / queued_blocks * 100:.2f}%)")
        print(f"This took {time_str}")
        print()
        LOGGER.info("Processed %s out of %s blocks)", blocked_num, queued_blocks)
        LOGGER.info("Processing took %s", time_str)
        LOGGER.info("processing + networking per block = %ss avg", blocked_num / time_total)


if __name__ == "__main__":
    PATHS = get_workdirs()
    for directory in PATHS.values():
        directory.mkdir(exist_ok=True)

    FH = logging.FileHandler(PATHS["data"] / "chainblocker.log", mode="w")
    FH.setLevel(logging.DEBUG)
    FH.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s: %(message)s"))
    LOGGER.addHandler(FH)
    try:
        main(paths=PATHS)
    except Exception as exc:
        # ignore argparse-issued systemexit
        if not isinstance(exc, SystemExit):
            LOGGER.exception("UNCAUGHT EXCEPTION:")
            EXCEPTION_LOG = PATHS["data"] / time.strftime("chainblocker_exception_%Y-%m-%dT_%H-%M-%S.log")
            shutil.copy(FH.baseFilename, EXCEPTION_LOG)
            print("Chainblocker quit due to unexpected error!")
            print(f"Error: {exc}")
            print(f"Traceback has been saved to {str(EXCEPTION_LOG)}")
            print("If this issue persists, please report it to the project's github repo: https://github.com/rmmbear/twitter-chain-blocker")
