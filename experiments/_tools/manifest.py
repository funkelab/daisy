"""The experiment index: one entry per experiment directory.

group: which migration table the row belongs to
claim: the one-sentence claim the script verifies
timeout: seconds before the capture tool declares a hang
"""

EXPERIMENTS = [
    # group, directory, claim, timeout
    (
        "same_behavior",
        "funlib_geometry_types",
        "funlib.geometry Roi/Coordinate work everywhere on the default surface: daisy.Roi is funlib's class, Task/Block accept them positionally and as keywords, blocks and task.total_roi hand them back.",
        60,
    ),
    (
        "same_behavior",
        "check_function_numpy_bool",
        "A check_function returning numpy.bool_(True) skips every block on both lines.",
        60,
    ),
    (
        "same_behavior",
        "check_function_raises",
        "A check_function that raises counts as 'not done' on both lines; 1.x logs it per block, 2.0 once per task.",
        60,
    ),
    (
        "same_behavior",
        "worker_sigkill",
        "A SIGKILLed worker is replaced and its block retried on both lines (1.x master; 1.2.2 hangs).",
        90,
    ),
    (
        "same_behavior",
        "serial_worker_function",
        "Serial mode refuses a 0-argument worker function on both lines.",
        60,
    ),
    (
        "v1_hangs_v2_finishes",
        "hung_block_timeout",
        "A block that never returns hangs 1.x forever; 2.0 kills it at Task(timeout=) and reports it failed.",
        45,
    ),
    (
        "v1_hangs_v2_finishes",
        "crashing_spawn_function",
        "A spawn function that exits at once is respawned forever on 1.x; 2.0 stops after its start budget and raises.",
        12,
    ),
    (
        "v1_hangs_v2_finishes",
        "fire_and_forget_launcher",
        "A launcher that returns at once, whose worker then dies, hangs 1.x; 2.0 fails the run with an explanation.",
        45,
    ),
    (
        "v1_hangs_v2_finishes",
        "log_dir_with_colon",
        "A log directory containing ':' breaks every 1.x external worker in a respawn storm; 2.0 escapes it and runs.",
        20,
    ),
    (
        "v1_hangs_v2_finishes",
        "zero_workers",
        "Zero workers waits forever on 1.x; 2.0 raises when the Task is built.",
        30,
    ),
    (
        "v1_hangs_v2_finishes",
        "unreachable_downstream",
        "A downstream task reaching beyond its upstream hangs 1.x after the reachable blocks; 2.0 refuses the pipeline.",
        45,
    ),
    (
        "v1_finishes_v2_abandons",
        "twenty_bad_blocks",
        "20 always-failing blocks of 2,000: 1.x finishes with 20 failed and returns True; 2.0 abandons the task after 14 worker deaths and raises.",
        300,
    ),
    (
        "v1_finishes_v2_abandons",
        "always_failing_remote_worker",
        "An external worker that fails every block: 1.x launches a new job every 4 failures and finishes; 2.0 launches one per failure until its budget is spent, then raises.",
        120,
    ),
    (
        "v1_finishes_v2_abandons",
        "late_failure_after_idle_release",
        "A failure after the other worker was released as idle: 1.x retries; 2.0 with max_worker_restarts=0 abandons with one orphan.",
        60,
    ),
    (
        "v1_silent_v2_loud",
        "return_value_with_failures",
        "With every block failed, 1.x returns True; 2.0 returns False.",
        60,
    ),
    (
        "v1_silent_v2_loud",
        "unpicklable_exception",
        "An exception that cannot be pickled leaves 1.x's failure list empty (and returns True); 2.0 carries the traceback as text.",
        60,
    ),
    (
        "v1_silent_v2_loud",
        "duplicate_task_ids",
        "Two tasks with one task_id: 1.x silently drops the second; 2.0 raises.",
        60,
    ),
    (
        "v1_silent_v2_loud",
        "ctrl_c",
        "Ctrl-C: 1.x returns False with a summary that hides the in-flight blocks; 2.0 prints what finished and raises KeyboardInterrupt.",
        60,
    ),
    (
        "v1_silent_v2_loud",
        "check_function_array",
        "A check_function returning an array aborts the whole 1.x run; 2.0 warns once and runs the block.",
        60,
    ),
    (
        "different_by_design",
        "default_timeout",
        "No timeout passed: 1.x runs an 11-minute block to completion; 2.0 kills it at the 600 s default and reports it failed.",
        1500,
    ),
    (
        "different_by_design",
        "all_blocks_already_done",
        "A run that skips every block prints an empty summary on 1.x and a table with a skipped column on 2.0.",
        60,
    ),
    (
        "different_by_design",
        "log_basedir_none",
        "set_log_basedir(None) reaches external workers as the string 'None' on 1.x, so a worker that redirects its output writes to ./None/; 2.0 ships an empty value meaning off.",
        60,
    ),
    (
        "different_by_design",
        "progress_output_volume",
        "With stderr redirected to a file, 1.x writes two progress refreshes per block; 2.0 refreshes on a timer.",
        300,
    ),
    (
        "different_by_design",
        "resume_check_function_vs_tracking",
        "Resuming 20,000 blocks: a check_function costs one call per block on the scheduling thread; 2.0's built-in store is one byte per block.",
        600,
    ),
]
