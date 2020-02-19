""""""
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

WORKING_DIR = Path.home() / "Twitter ChainBlocker"
WORKING_DIR.mkdir(exist_ok=True)


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
    BlocklistDBBase.metadata.create_all(sqla_engine)
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

        #TODO: add confirmation dialogues for blocking and unblocking

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
