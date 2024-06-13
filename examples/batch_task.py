import pymongo
import sqlite3
import json
import hashlib
import os
import argparse
import copy
import pkg_resources
import logging

import daisy

daisy_version = float(pkg_resources.get_distribution("daisy").version)
assert daisy_version >= 1, (
    f"This script was written for daisy v1.0 but current installed version "
    f"is {daisy_version}"
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("BatchTask")


class Database:

    def __init__(self, db_host, db_id, overwrite=False):
        self.table_name = "completion_db_col"

        if db_host is None:
            # Use SQLite
            self.use_sql = True
            os.makedirs("daisy_db", exist_ok=True)
            self.con = sqlite3.connect(f"daisy_db/{db_id}.db", check_same_thread=False)
            self.cur = self.con.cursor()

            if overwrite:
                self.cur.execute(f"DROP TABLE IF EXISTS {self.table_name}")
                self.con.commit()

            # check if table exists
            self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [k[0] for k in self.cur.fetchall()]
            if self.table_name not in tables:
                self.cur.execute(f"CREATE TABLE {self.table_name} (block_id text)")
                self.con.commit()

        else:
            # Use MongoDB
            self.use_sql = False
            self.client = pymongo.MongoClient(db_host)

            if overwrite:
                self.client.drop_database(db_id)

            db = self.client[db_id]
            if self.table_name not in db.list_collection_names():
                self.completion_db = db[self.table_name]
                self.completion_db.create_index(
                    [("block_id", pymongo.ASCENDING)], name="block_id"
                )
            else:
                self.completion_db = db[self.table_name]

    def check(self, block_id):

        if self.use_sql:
            block_id = "_".join([str(s) for s in block_id])
            res = self.cur.execute(
                f"SELECT * FROM {self.table_name} where block_id = '{block_id}'"
            ).fetchall()
            if len(res):
                return True
        else:
            if self.completion_db.count_documents({"block_id": block_id}) >= 1:
                return True

        return False

    def add_finished(self, block_id):

        if self.use_sql:
            block_id = "_".join([str(s) for s in block_id])
            self.cur.execute(f"INSERT INTO {self.table_name} VALUES ('{block_id}')")
            self.con.commit()
        else:
            document = {"block_id": block_id}
            self.completion_db.insert_one(document)


class BatchTask:
    """Example base class for a batchable Daisy task.

    This class takes care of some plumbing work such as creating a database
    to keep track of finished block and writing a config and batch file
    that users can submit jobs to a job system like SLURM.

    Derived tasks will only need to implement the code for computing the task.
    """

    @staticmethod
    def parse_args(ap):

        try:
            ap.add_argument(
                "--db_host",
                type=str,
                help="MongoDB database host name. If `None` (default), use SQLite",
                default=None,
            )
            # default='10.117.28.139')
            ap.add_argument(
                "--db_name",
                type=str,
                help="MongoDB database project name",
                default=None,
            )
            ap.add_argument(
                "--overwrite",
                type=int,
                help="Whether to overwrite completed blocks",
                default=0,
            )
            ap.add_argument(
                "--num_workers", type=int, help="Number of workers to run", default=4
            )
            ap.add_argument(
                "--no_launch_workers",
                type=int,
                help="Whether to run workers automatically",
                default=0,
            )
            ap.add_argument(
                "--config_hash",
                type=str,
                help="config string, used to keep track of progress",
                default=None,
            )
            ap.add_argument(
                "--task_name",
                type=str,
                help="Name of task, default to class name",
                default=None,
            )
        except argparse.ArgumentError as e:
            print("Current task has conflicting arguments with BatchTask!")
            raise e

        return vars(ap.parse_args())

    def __init__(self, config=None, config_file=None, task_id=None):

        if config_file:
            print(f"Loading from config_file: {config_file}")
            with open(config_file, "r") as f:
                config = json.load(f)

        assert config is not None

        for key in config:
            setattr(self, "%s" % key, config[key])

        if self.task_name is None:
            self.task_name = str(self.__class__.__name__)

        self.__init_config = copy.deepcopy(config)

        if self.config_hash is None:
            config_str = "".join(
                [
                    "%s" % (v,)
                    for k, v in config.items()
                    if k not in ["overwrite", "num_workers", "no_launch_workers"]
                ]
            )
            self.config_hash = str(hashlib.md5(config_str.encode()).hexdigest())
            config_hash_short = self.config_hash[0:8]

        self.db_id = "%s_%s" % (self.task_name, self.config_hash)
        db_id_short = "%s_%s" % (self.task_name, config_hash_short)

        # if not given, we need to give the task a unique id for chaining
        if task_id is None:
            task_id = db_id_short
        self.task_id = task_id

        self.write_config_called = False

        self._task_init()

    def _task_init(self, config):
        assert False, "Function needs to be implemented by subclass"

    def prepare_task(self):
        """Called by user to get a `daisy.Task`. It should call
        `_write_config()` and return with a call to `_prepare_task()`

        Returns:
            `daisy.Task` object
        """
        assert False, "Function needs to be implemented by subclass"

    def _write_config(self, worker_filename, extra_config=None):
        """Make a config file for workers. Workers can then be run on the
        command line on potentially a different machine and use this file
        to initialize its variables.

        Args:
            extra_config (``dict``, optional):
                Any extra configs that should be written for workers
        """

        config = self.__init_config
        if extra_config:
            for k in extra_config:
                config[k] = extra_config[k]

        self.config_file = os.path.join(".run_configs", "%s.config" % self.db_id)

        self.new_actor_cmd = "python %s run_worker %s" % (
            worker_filename,
            self.config_file,
        )

        if self.db_name is None:
            self.db_name = "%s" % self.db_id

        config["db_id"] = self.db_id

        os.makedirs(".run_configs", exist_ok=True)
        with open(self.config_file, "w") as f:
            json.dump(config, f)

        # write batch script
        self.sbatch_file = os.path.join(".run_configs", "%s.sh" % self.db_id)
        self.generate_batch_script(
            self.sbatch_file,
            self.new_actor_cmd,
            log_dir=".logs",
            logname=self.db_id,
        )

        self.write_config_called = True

    def generate_batch_script(
        self,
        output_script,
        run_cmd,
        log_dir,
        logname,
        cpu_time=11,
        queue="short",
        cpu_cores=1,
        cpu_mem=2,
        gpu=None,
    ):
        """Example SLURM script."""

        text = []
        text.append("#!/bin/bash")
        text.append("#SBATCH -t %d:40:00" % cpu_time)

        if gpu is not None:
            text.append("#SBATCH -p gpu")
            if gpu == "" or gpu == "any":
                text.append("#SBATCH --gres=gpu:1")
            else:
                text.append("#SBATCH --gres=gpu:{}:1".format(gpu))
        else:
            text.append("#SBATCH -p %s" % queue)
        text.append("#SBATCH -c %d" % cpu_cores)
        text.append("#SBATCH --mem=%dGB" % cpu_mem)
        text.append("#SBATCH -o {}/{}_%j.out".format(log_dir, logname))
        text.append("#SBATCH -e {}/{}_%j.err".format(log_dir, logname))

        text.append("")
        text.append(run_cmd)

        with open(output_script, "w") as f:
            f.write("\n".join(text))

    def _prepare_task(
        self,
        total_roi,
        read_roi,
        write_roi,
        check_fn=None,
        fit="shrink",
        read_write_conflict=False,
        upstream_tasks=None,
    ):

        assert self.write_config_called, "`BatchTask._write_config()` was not called"

        print(
            "Processing total_roi %s with read_roi %s and write_roi %s"
            % (total_roi, read_roi, write_roi)
        )

        if check_fn is None:
            check_fn = self._default_check_fn

        if self.overwrite:
            print("Dropping table %s" % self.db_id)

            if self.overwrite == 2:
                i = "Yes"
            else:
                i = input("Sure? Yes/[No] ")

            if i == "Yes":
                print("Dropped %s!" % self.db_id)
            else:
                print("Aborted")
                exit(0)

        self.database = Database(self.db_host, self.db_id, overwrite=self.overwrite)

        return daisy.Task(
            task_id=self.task_id,
            total_roi=total_roi,
            read_roi=read_roi,
            write_roi=write_roi,
            process_function=self._new_worker,
            read_write_conflict=read_write_conflict,
            fit=fit,
            num_workers=self.num_workers,
            max_retries=1,
            check_function=check_fn,
            init_callback_fn=self.init_callback_fn,
            upstream_tasks=upstream_tasks,
        )

    def _default_check_fn(self, block):
        """The default check function uses database for checking completion"""

        if self.overwrite:
            return False

        return self.database.check(block.block_id)

    def _worker_impl(self, args):
        """Worker function implementation"""
        assert False, "Function needs to be implemented by subclass"

    def _new_worker(self):

        if not self.no_launch_workers:
            self.run_worker()

    def run_worker(self):
        """Wrapper for `_worker_impl()`"""

        assert (
            "DAISY_CONTEXT" in os.environ
        ), "DAISY_CONTEXT must be defined as an environment variable"
        logger.info("WORKER: Running with context %s" % os.environ["DAISY_CONTEXT"])

        database = Database(self.db_host, self.db_id)
        client_scheduler = daisy.Client()

        while True:
            with client_scheduler.acquire_block() as block:
                if block is None:
                    break
                logger.info(f"Received block {block}")
                self._worker_impl(block)
                database.add_finished(block.block_id)

    def init_callback_fn(self, context):

        print(
            "sbatch command: DAISY_CONTEXT={} sbatch --parsable {}\n".format(
                context.to_env(), self.sbatch_file
            )
        )

        print(
            "Terminal command: DAISY_CONTEXT={} {}\n".format(
                context.to_env(), self.new_actor_cmd
            )
        )
