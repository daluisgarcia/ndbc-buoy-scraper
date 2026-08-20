"""Package init -- loads .env before anything reads os.environ.

Every entry point in this project (scrapy settings, the silver job, the
healthcheck, pg) lives inside this package, so Python imports THIS module
first no matter which one is invoked. That makes it the one place where .env
has to be loaded, instead of a load_dotenv() call in each entry point that a
future one would forget.

Two deliberate choices:

  * override=False (the default) -- a variable already in the real process
    environment WINS over .env. That ordering is what makes
    `NDBC_FULL_REFRESH=1 make pipeline` and systemd's EnvironmentFile work:
    an explicit override at invocation time must not be silently replaced by
    the checked-in development value.

  * an explicit path anchored to the repo, not the default cwd search. The
    silver job and the spider are launched from different working directories
    (make -C, systemd WorkingDirectory), and a cwd-relative lookup makes
    "which .env did it actually read?" depend on how you invoked it.
"""

from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"

# Missing .env is not an error: every variable has a working default in code,
# so a fresh clone runs without one.
load_dotenv(ENV_FILE)
