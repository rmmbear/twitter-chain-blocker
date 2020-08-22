import logging

LOGGER = logging.getLogger()
FH = logging.FileHandler("./chainblocker_tests.log", mode="w")
FH.setLevel(logging.DEBUG)
FH.setFormatter(logging.Formatter("[%(levelname)s] %(asctime)s: %(message)s"))
LOGGER.addHandler(FH)
