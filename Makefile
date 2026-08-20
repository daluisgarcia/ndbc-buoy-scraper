.PHONY: scrape up silver health pipeline test down db-config

# Pipeline order: scrape (host, Scrapy) -> silver (host, DuckDB). Each stage
# reads the previous stage's output off the shared ./data bind mount; none
# of them re-does a prior stage's work.

# Host-side scrape (Python 3.13/uv, NOT containerized): lands bronze parquet
# under data/bronze/ (design section 5 — scraper stays decoupled from Docker).
# NOTE: the `scrapy` console script is not on PATH in this env; the module
# form (`python -m scrapy`) is the verified-working invocation.
scrape:
	@echo "Running Scrapy spider on host -> writing bronze parquet to data/bronze/"
	uv run python -m scrapy crawl ndbc_standard_meterological

# Start Postgres only, waiting for its healthcheck (db/init.sql runs
# automatically on first init against a fresh pgdata volume).
up:
	@echo "Starting Postgres (db/init.sql applies on first init)"
	docker compose up -d postgres

# Run the DuckDB silver job (host-side, Python 3.13/uv, NOT containerized):
# reads bronze, applies all semantic transforms, and rebuilds the Postgres
# year partitions whose bronze changed -- see ndbc_buoy_scraper/silver.py.
# Set SILVER_FORCE=1 to rebuild every year regardless.
silver:
	@echo "Running DuckDB silver job -> rebuilding changed year partitions"
	uv run python -m ndbc_buoy_scraper.silver

# Freshness + last-run probe. Exits non-zero when unhealthy, so it works as a
# monitoring check as-is -- run it after a run, or from an external probe.
health:
	uv run python -m ndbc_buoy_scraper.healthcheck

# Full pipeline, in order. This is what `run-bg` executes detached.
pipeline: scrape silver

# Run the full pytest suite on the host. All tests run on the host now (no
# containerized runtime split).
test:
	@echo "Running pytest suite on the host"
	uv run pytest tests/

# Stop the stack. Note: this does NOT wipe the pgdata named volume; use
# `docker compose down -v` manually to also drop Postgres data.
down:
	@echo "Stopping the Docker Compose stack"
	docker compose down

# Print the settings that prove the tuned config actually loaded. Seeing the
# stock 128MB/1GB here means Postgres started on its built-in defaults, i.e.
# db/postgresql.prod.conf did not mount.
#
# There is no separate prod-up target: docker-compose.yml is the same file on
# both machines now, so `make up` is the only way to start it.
db-config:
	docker compose exec -T postgres psql -U ndbc -d ndbc \
		-c "SHOW shared_buffers;" -c "SHOW max_wal_size;" \
		-c "SHOW synchronous_commit;" -c "SHOW work_mem;"

# --- Detached runs (server) ------------------------------------------------
#
# Run the pipeline so it survives the SSH session that started it. Linux/systemd
# only -- these fail on a developer Mac, by design.
#
# `make pipeline &` would NOT do this: the run stays a child of the shell, so a
# disconnect SIGHUPs the whole tree. systemd-run hands the process to PID 1 and
# journald takes the logs, which is why there is no redirect or logrotate config
# anywhere in here.
#
# There is no schedule and no installed unit -- the unit is created on demand
# and --collect removes it once it exits, so RUN_UNIT is reusable immediately.

RUN_UNIT ?= ndbc-run

.PHONY: run-bg logs status stop

# Start a detached run and return immediately.
#
# No EnvironmentFile= here: ndbc_buoy_scraper/__init__.py loads .env itself, so
# passing the same file through systemd as well would be two mechanisms feeding
# one set of variables -- and the next person to debug a wrong value would have
# to know about both.
run-bg:
	sudo systemd-run --collect --unit=$(RUN_UNIT) \
		--working-directory=$(CURDIR) \
		/usr/bin/make -C $(CURDIR) pipeline
	@echo "Started as $(RUN_UNIT). Follow with: make logs"

# Follow the run. Ctrl-C or a dropped connection stops the tail, never the run.
logs:
	journalctl -u $(RUN_UNIT) -f

# Still running, or how did it end?
status:
	systemctl status $(RUN_UNIT) --no-pager

stop:
	sudo systemctl stop $(RUN_UNIT)
