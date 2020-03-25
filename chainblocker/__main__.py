""""""
import os
import time
import shutil
import logging
from typing import Optional

from pathlib import Path
from argparse import ArgumentParser

import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker

from chainblocker import *

LOGGER = logging.getLogger(__name__)

ARGPARSER = ArgumentParser(
    prog="chainblocker",
    description="All account arguments must be passed in form of screen names (a.k.a 'handles'), "
                "and not display names or IDs. Screen names are resolved to IDs internally, which "
                "means that this program will work even when blocked users change their account names. "
                "If, for any reason, chainblocker was stopped while processing queues and you would "
                "like to resume without adding anything to the queue, simply run it again without a command")
ARGPARSER.add_argument(
    "--skip-blocklist-update", action="store_true",
    help="Do not update account's blocklist before queueing/processing.")
ARGPARSER.add_argument(
    "--unblocks-first", action="store_true", help="Process unblock queue before the block queue")
ARGPARSER.add_argument(
    "--only-queue-accounts", action="store_true",
    help="Delay queue processing until next run and only queue accounts for blocking/unblocking.")
ARGPARSER.add_argument(
    "--only-queue-action", action="store_true",
    help="Delay queueing accounts until next run and only store which actions to perform. "
         "Useful if you want to issue different commands one after another, and don't want to wait for account queueing. "
         "This option also disables blocklist update")
ARGPARSER.add_argument(
    "--mode", nargs=1, type=str, default="target+followers",
    help="Set which parties will be affected in current batch of accounts. "
         "Options must be delimited with a '+' symbol. Possible options are: "
         "target = the account named, followers = target's followers, followed = people followed by target. "
         "Mode defaults to target+followers for both blocking and unblocking")
ARGPARSER.add_argument(
    "--comment", nargs=1, type=str,
    help="Set the comment for this batch operation. This comment will be displayed when querying block reason. "
         "If left empty, comment will be automatically set to "
         "'Session {year}/{month}/{day} {hours}:{minutes}:{seconds}, queried {number} accounts'")

ARGP_COMMANDS = ARGPARSER.add_subparsers(title="Commands", dest="command", metavar="")
ARGP_BLOCK = ARGP_COMMANDS.add_parser(
    "block",
    help="Block specified accounts and their followers (use --mode to change this behavior)")
ARGP_BLOCK.add_argument(
    "accounts", nargs="*",
    help="List of screen names of accounts you wish to block")
ARGP_UNBLOCK = ARGP_COMMANDS.add_parser(
    "unblock",
    help="Unblock (or remove from the block queue) specified accounts and their followers "
         "(use --mode to change this behavior, for example: '--mode followers' to only unblock "
         "followers of the target account, but keep the target account blocked")
ARGP_UNBLOCK.add_argument(
    "accounts", nargs="*",
    help="List of screen names of accounts you wish to unblock.")
ARGP_REASON = ARGP_COMMANDS.add_parser("reason", help="Check if you are blocking someone and display details of that block")
ARGP_REASON.add_argument(
    "account_name", nargs=1,
    help="Screen name of the account you want to query")


#TODO: implement show_user_info -just pretty print the User object + number of blocked users + block reason
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
    #FIXME: implement all arguments
    NOT_IMPLEMENTED = ["unblocks_first", "only_queue_action", "mode", "comment"]
    for missing in NOT_IMPLEMENTED:
        if getattr(args, missing, None):
            raise NotImplementedError(f"'{missing}' is not yet implemented")

    #FIXME: implement unblocking
    if args.command == "unblock":
        raise NotImplementedError(f"unblocking is not yet implemented")
    #FIXME: implement reason
    if args.command == "reason":
        raise NotImplementedError(f"reason is not yet implemented")

    current_user = AuthedUser.authenticate_interactive()

    dbfile = paths["data"] / f"{current_user.user.id}_blocklist.sqlite"
    sqla_engine = sqla.create_engine(f"sqlite:///{str(dbfile)}", echo=False)
    BlocklistDBBase.metadata.create_all(sqla_engine)
    bound_session = sessionmaker(bind=sqla_engine)

    LOGGER.info("Creating new db session")
    session_start = time.time()
    db_session = bound_session()

    print("Current blocklist statistics:")
    for name, count in blocks_status(db_session).items():
        print(f"{name} : {count}")

    print()

    blocks_queued = 0
    try:
        clean_exit = Metadata.get_row("clean_exit", db_session, "1")
        if clean_exit.val == "0":
            LOGGER.warning("Exception encountered in last session, performing maintenance")
            db_maintenance(db_session)

        if args.command == "unblock":
            pass

        if args.command == "block":
            print("getting authenticated user's follows...")
            authenticated_user_follows = [x for x in current_user.get_followed_ids(current_user.user.id)]
            if not args.skip_blocklist_update:
                update_blocklist(current_user, db_session)

            #TODO: add confirmation dialogues for blocking and unblocking

            for account_name in args.accounts:
                target_user = current_user.get_user(screen_name=account_name)
                LOGGER.info("Queueing blocks for followers of USER=%s ID=%s", target_user.screen_name, target_user.id)
                blocks_queued += block_followers_of(
                    authed_usr=current_user, target_user=target_user, db_session=db_session,
                    block_followers=(not args.dont_block_followers), block_target=(not args.dont_block_target),
                    block_following=(args.block_targets_followed), whitelisted_accounts=authenticated_user_follows)

            print(f"Added {blocks_queued} new accounts to block queue")

        if not args.only_queue_accounts:
            print("Processing block queue")
            process_block_queue(current_user, db_session, authenticated_user_follows)
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
            exception_log = PATHS["data"] / time.strftime("chainblocker_exception_%Y-%m-%dT_%H-%M-%S.log")
            shutil.copy(FH.baseFilename, exception_log)
            print("Chainblocker quit due to unexpected error!")
            print(f"Error: {exc}")
            print(f"Traceback has been saved to {str(exception_log)}")
            print("If this issue persists, please report it to the project's github repo: https://github.com/rmmbear/twitter-chain-blocker")
