#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import argparse
import logging
import threading
import subprocess
import tempfile
import uuid
from datetime import datetime

# ==============================================================================
# 1. Utilities: Tracker & Monitor Dashboard
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, execution_id, total_task, log_path, output_sh):
        self.execution_id = execution_id
        self.total_task = total_task
        self.log_path = log_path
        self.output_sh = output_sh
        self.completed_task = 0
        self.results = []
        self.current_action = "Initializing..."
        self.start_time = time.time()
        self.lock = threading.Lock()

    def update_action(self, action_msg):
        with self.lock:
            self.current_action = action_msg

    def add_result(self, table_name, status, remark="-"):
        with self.lock:
            self.results.append({
                'table': table_name,
                'status': status,
                'remark': str(remark)
            })
            self.completed_task += 1

    def get_dashboard_data(self):
        with self.lock:
            return self.completed_task, self.total_task, self.current_action

    def print_summary(self, logger):
        add_count = sum(1 for r in self.results if r['status'] == 'ADD')
        skip_count = sum(1 for r in self.results if r['status'] == 'SKIP')
        force_count = sum(1 for r in self.results if r['status'] == 'FORCE')
        dry_count = sum(1 for r in self.results if r['status'] == 'DRY-RUN')
        
        elapsed_secs = int(time.time() - self.start_time)
        h, rem = divmod(elapsed_secs, 3600)
        m, s = divmod(rem, 60)
        duration_str = "{0:02d}:{1:02d}:{2:02d}".format(h, m, s)

        summary_lines = [
            "",
            "="*90, # Expanded width for paths
            " FINAL SUMMARY REPORT : {0}".format(self.execution_id),
            "="*90,
            " Total Tables Processed : {0}".format(len(self.results)),
            " Added to Deletion List : {0} (Includes FORCE/DRY-RUN Mode)".format(add_count + force_count + dry_count),
            " Skipped                : {0}".format(skip_count),
            " Duration               : {0}".format(duration_str),
            "-"*90,
            " Log File               : {0}".format(self.log_path),   # ADDED
            " Output Script          : {0}".format(self.output_sh), # ADDED
            "="*90
        ]
        
        for line in summary_lines:
            logger.info(line)
            print(line)

class MonitorThread(threading.Thread):
    def __init__(self, tracker):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard() # Print one last time before exiting

    def print_dashboard(self):
        comp, total, action = self.tracker.get_dashboard_data()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        lines = [
            "="*120,
            " HDFS HOUSEKEEPING MONITOR (Python 2.7) ",
            " Execution ID: {0}".format(self.tracker.execution_id),
            "="*120,
            " Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct),
            " Elapsed : {0:.0f}s".format(elapsed),
            " Action  : {0}".format(action)[:119], # Keep truncation for action to prevent terminal layout break
            "-"*120,
            " Log Path: {0}".format(self.tracker.log_path),   # REMOVED [:89]
            " Out Path: {0}".format(self.tracker.output_sh),  # REMOVED [:89]
            "="*120
        ]

        if not self.first_print:
            # Move cursor up by the number of lines to overwrite (ANSI Escape)
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        # Clear line and print
        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        sys.stdout.write(output)
        sys.stdout.flush()

# ==============================================================================
# 2. Config & Logging
# ==============================================================================

class HousekeepingLogger(object):
    @staticmethod
    def setup(log_dir, global_ts):
        try:
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_file = os.path.join(log_dir, "housekeeping_{0}.log".format(global_ts))
            
            logger = logging.getLogger("HousekeepingJob")
            logger.setLevel(logging.INFO)
            logger.handlers = []
            
            fh = logging.FileHandler(log_file)
            fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            logger.addHandler(fh)
            
            return logger, log_file
        except Exception as e:
            print("CRITICAL ERROR: Cannot create log directory/file. Error: {0}".format(e))
            sys.exit(1)

class ConfigManager(object):
    def __init__(self, logger, project_root):
        self.logger = logger
        self.project_root = project_root
        self.args = None
        self.hdfs_prefix = ""
        self.retention_hours = 24
        self.kinit_cmd = ""
        self.table_list = []

    def parse_args(self):
        parser = argparse.ArgumentParser(description="HDFS Parquet Housekeeping")
        parser.add_argument('--config', default='env_config.txt', help='Path or filename for env_config.txt')
        parser.add_argument('--list', default='table_list.txt', help='Path or filename for table list file')
        parser.add_argument('--force', action='store_true', help='Bypass retention age check')
        parser.add_argument('--dry-run', action='store_true', help='Simulate execution without deleting')
        self.args = parser.parse_args()

        config_dir = os.path.join(self.project_root, 'config')

        if not os.path.isabs(self.args.config):
            self.args.config = os.path.join(config_dir, self.args.config)
            
        if not os.path.isabs(self.args.list):
            self.args.list = os.path.join(config_dir, self.args.list)

    def load_configs(self):
        self.logger.info("--- [1/3] Input Parameters ---")
        self.logger.info("Config File : {0}".format(self.args.config))
        self.logger.info("List File   : {0}".format(self.args.list))
        self.logger.info("Force Mode  : {0}".format(self.args.force))
        self.logger.info("Dry-Run Mode: {0}".format(self.args.dry_run))
        self.logger.info("------------------------------")

        # 1. Load env_config.txt (Remains the same)
        try:
            with open(self.args.config, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        key, val = [x.strip() for x in line.split('=', 1)]
                        if key == 'hdfs_prefix': self.hdfs_prefix = val
                        elif key == 'retention_hours': self.retention_hours = float(val)
                        elif key == 'kinit': self.kinit_cmd = val
            self.logger.info("--- [2/3] Environment Config ---")
            self.logger.info("HDFS Prefix     : {0}".format(self.hdfs_prefix))
            self.logger.info("Retention Hours : {0}".format(self.retention_hours))
            self.logger.info("Kinit Command   : {0}".format(self.kinit_cmd if self.kinit_cmd else "None"))
            self.logger.info("--------------------------------")
            if self.kinit_cmd:
                self.logger.info("Loaded Config -> Kinit Command: Found")
        except Exception as e:
            self.logger.critical("Failed to read config file: {0}".format(e))
            raise

        # 2. Load Table List & Parse string to Nested Folder
        try:
            with open(self.args.list, 'r') as f:
                for line_num, line in enumerate(f, 1):
                    raw_line = line.strip()
                    if not raw_line or raw_line.startswith('#'): continue
                    
                    try:
                        db_part, tbl_part = raw_line.split('|')
                        sch_part, real_tbl = tbl_part.split('.')
                        
                        db_clean = db_part.strip()
                        sch_clean = sch_part.strip()
                        tbl_clean = real_tbl.strip()
                        
                        if not db_clean or not sch_clean or not tbl_clean:
                            raise ValueError("Missing component")
                            
                        nested_path = "{0}/{1}/{2}".format(db_clean, sch_clean, tbl_clean)
                        
                        # Store both raw name (for logs) and nested path (for HDFS)
                        self.table_list.append({
                            'raw_name': raw_line,
                            'nested_path': nested_path
                        })
                        
                    except ValueError:
                        # Handle missing '|' or '.' gracefully without crashing the whole script
                        self.logger.warning("Line {0} skipped (Invalid format): '{1}'. Expected 'db|schema.table'".format(line_num, raw_line))

            self.logger.info("--- [3/3] Table List Input ---")
            self.logger.info("Total Tables Loaded: {0}".format(len(self.table_list)))
            for idx, tbl in enumerate(self.table_list, 1):
                self.logger.info("  {0:03d}. {1} -> {2}".format(idx, tbl['raw_name'], tbl['nested_path']))
            self.logger.info("------------------------------")
        except Exception as e:
            self.logger.critical("Failed to read table list file: {0}".format(e))
            raise

        if not self.table_list:
            raise ValueError("Table list is empty or invalid. Aborting.")
        
# ==============================================================================
# 3. Core Logic: HDFS & Command Generator
# ==============================================================================

class HdfsHelper(object):
    def __init__(self, logger):
        self.logger = logger

    def get_modified_timestamp(self, hdfs_path):
        """ Returns unix timestamp in milliseconds, or None if error/not found """
        cmd = 'hdfs dfs -stat "%Y" {0}'.format(hdfs_path)
        
        self.logger.info("Executing HDFS Command: {0}".format(cmd))
        
        try:
            # stderr=subprocess.STDOUT merges stderr into stdout
            output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
            
            # Decode for compatibility and clean string
            result_str = output.decode('utf-8', 'ignore').strip() if hasattr(output, 'decode') else output.strip()
            self.logger.info("[HDFS_STDOUT] Result: {0}".format(result_str))
            
            return long(result_str)
            
        except subprocess.CalledProcessError as e:
            error_msg = e.output.decode('utf-8', 'ignore').strip() if e.output else "No output returned."
            self.logger.warning("[HDFS_STDERR] Failed (Exit Code {0}): {1}".format(e.returncode, error_msg))
            return None
            
        except Exception as e:
            self.logger.error("[HDFS_ERROR] Unexpected error checking HDFS stat: {0}".format(e), exc_info=True)
            return None

    def is_expired(self, hdfs_path, retention_hours):
        modified_ms = self.get_modified_timestamp(hdfs_path)
        if not modified_ms:
            return False, "Not Found or Error"

        current_ms = long(time.time() * 1000)
        age_ms = current_ms - modified_ms
        retention_ms = long(retention_hours * 3600 * 1000)

        modified_date_str = datetime.fromtimestamp(modified_ms / 1000.0).strftime('%Y-%m-%d %H:%M:%S')

        age_hours = age_ms / (3600.0 * 1000)
        
        # Calculate age format
        if age_hours >= 24:
            total_mins = int(age_ms / (60 * 1000))
            days, rem_mins = divmod(total_mins, 1440)
            hours, mins = divmod(rem_mins, 60)
            age_str = "{0:.2f}h | {1}d {2}h {3}m".format(age_hours, days, hours, mins)
        else:
            age_str = "{0:.2f}h".format(age_hours)

        if age_ms > retention_ms:
            remark = "Expired (Age: {0}, Last Modified: {1})".format(age_str, modified_date_str)
            return True, remark
        else:
            remark = "Not Expired (Age: {0}, Last Modified: {1})".format(age_str, modified_date_str)
            return False, remark
        
class CommandGenerator(object):
    def __init__(self, config, hdfs_helper, tracker, logger):
        self.config = config
        self.hdfs_helper = hdfs_helper
        self.tracker = tracker
        self.logger = logger

    def sanitize_path(self, prefix, table_name):
        # Convert backslash to forward slash
        p = prefix.replace('\\', '/')
        t = table_name.replace('\\', '/')
        # Strip trailing/leading slashes to prevent //
        p = p.rstrip('/')
        t = t.lstrip('/')
        return "{0}/{1}".format(p, t)

    def write_command_block(self, file_obj, table_name, hdfs_path):
        file_obj.write('echo "=================================================="\n')
        file_obj.write('echo "[START] {0} : {1}"\n'.format(table_name, hdfs_path))
        file_obj.write('echo "=================================================="\n')
        # Fail-Fast logic implemented via || exit 1
        file_obj.write('hdfs dfs -rm -f {0}/*/*.parquet || exit 1\n'.format(hdfs_path))
        file_obj.write('hdfs dfs -rm -f {0}/*.parquet || exit 1\n'.format(hdfs_path))
        file_obj.write('hdfs dfs -rmdir {0} || exit 1\n'.format(hdfs_path))
        file_obj.write('echo "[SUCCESS] {0} deleted."\n\n'.format(table_name))

    def generate(self, output_sh_path):
        self.logger.info("Phase 1: Starting Command Generation...")
        
        try:
            with open(output_sh_path, 'w') as sh_file:
                sh_file.write("#!/bin/bash\n")
                sh_file.write("# Auto-generated HDFS Housekeeping Script\n\n")

                for table_dict in self.config.table_list:
                    table = table_dict['raw_name']          # e.g., prodgp|prod_mis_drm.table_name
                    nested_path = table_dict['nested_path'] # e.g., prodgp/prod_mis_drm/table_name

                    # 1. Update Monitor
                    self.tracker.update_action("Checking: {0}".format(table))
                    
                    # 2. Path Sanitization (using nested_path instead of raw name)
                    target_path = self.sanitize_path(self.config.hdfs_prefix, nested_path)

                    # 3. Check Force Mode
                    if self.config.args.force:
                        self.write_command_block(sh_file, table, target_path)
                        if self.config.args.dry_run:
                            self.logger.info("[DRY-RUN FORCE] {0} - Bypassing age check".format(table))
                            self.tracker.add_result(table, 'DRY-RUN', 'Bypassed by --force')
                        else:
                            self.logger.info("[FORCE] {0} - Bypassing age check".format(table))
                            self.tracker.add_result(table, 'FORCE', 'Bypassed by --force')
                        continue

                    # 4. Normal Retention Check
                    is_exp, remark = self.hdfs_helper.is_expired(target_path, self.config.retention_hours)
                    
                    if is_exp:
                        # MODIFIED: Write command only once
                        self.write_command_block(sh_file, table, target_path)
                        
                        if self.config.args.dry_run:
                            self.logger.info("[DRY-RUN ADD] {0} - {1}".format(table, remark))
                            self.tracker.add_result(table, 'DRY-RUN', remark) 
                        else:
                            self.logger.info("[ADD] {0} - {1}".format(table, remark))
                            self.tracker.add_result(table, 'ADD', remark)
                    else:
                        self.logger.info("[SKIP] {0} - {1}".format(table, remark))
                        self.tracker.add_result(table, 'SKIP', remark)
                        
        except Exception as e:
            self.logger.error("Failed during shell script generation: {0}".format(e), exc_info=True)
            raise

# ==============================================================================
# 4. Phase 2: Automated Execution
# ==============================================================================

class ShellExecutor(object):
    def __init__(self, logger):
        self.logger = logger

    def execute(self, sh_path):
        self.logger.info("Phase 2: Executing generated shell script...")
        try:
            # Using Popen to capture stdout/stderr and handle Fail-Fast
            process = subprocess.Popen(['bash', sh_path], 
                                       stdout=subprocess.PIPE, 
                                       stderr=subprocess.PIPE)
            out, err = process.communicate()

            if process.returncode != 0:
                self.logger.error("[SHELL_ERROR] Script failed with exit code {0}".format(process.returncode))
                self.logger.error(">> STDERR DETAILS:\n{0}".format(err))
                print("\n[!] EXECUTION FAILED! Check log for stderr details.")
            else:
                self.logger.info("[SHELL_SUCCESS] All commands executed successfully.")

        except Exception as e:
            self.logger.error("Critical error while executing shell script: {0}".format(e), exc_info=True)

# ==============================================================================
# Main Controller
# ==============================================================================

class HousekeepingJob(object):
    def __init__(self):
        self.global_date = datetime.now().strftime("%Y%m%d")
        self.global_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.short_uuid = str(uuid.uuid4()).split('-')[0]
        self.execution_id = "HK_{0}_{1}".format(self.global_ts, self.short_uuid)
        
        # Setup paths
        self.main_path = os.path.dirname(os.path.abspath(__file__))
        self.project_root = os.path.dirname(self.main_path)
        self.log_dir = os.path.join(self.project_root, 'log', self.global_date)
        self.output_dir = os.path.join(self.project_root, 'output', self.global_date)

        if not os.path.exists(self.output_dir):
            try:
                os.makedirs(self.output_dir)
            except OSError as e:
                print("CRITICAL ERROR: Cannot create output directory: {0}".format(e))
                sys.exit(1)

        self.output_sh = os.path.join(self.output_dir, "execute_housekeeping_{0}.sh".format(self.global_ts))

        # Init Logger
        self.logger, self.log_path = HousekeepingLogger.setup(self.log_dir, self.global_ts)
        self.logger.info("Initializing Housekeeping Job ID: {0}".format(self.execution_id))

        # Init Config
        self.config = ConfigManager(self.logger, self.project_root)
        self.config.parse_args()
        self.config.load_configs()

        if self.config.kinit_cmd:
            self._authenticate_kerberos()

        # Init Trackers & Handlers
        self.tracker = ProcessTracker(self.execution_id, len(self.config.table_list), self.log_path, self.output_sh)
        self.hdfs_helper = HdfsHelper(self.logger)
        self.generator = CommandGenerator(self.config, self.hdfs_helper, self.tracker, self.logger)
        self.executor = ShellExecutor(self.logger)

    def _authenticate_kerberos(self):
        max_retries = 2
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info("Executing Kerberos authentication (Attempt {0}/{1})...".format(attempt, max_retries))
                subprocess.check_output(self.config.kinit_cmd, shell=True, stderr=subprocess.STDOUT)
                self.logger.info("Kerberos authentication successful.")
                return
            except subprocess.CalledProcessError as e:
                # Python 2.7 compatible decode with ignore
                error_msg = e.output.decode('utf-8', 'ignore') if e.output else str(e)
                if attempt < max_retries:
                    self.logger.warning("Kerberos authentication failed on attempt {0}. Retrying... Error: {1}".format(attempt, error_msg))
                    time.sleep(1)
                else:
                    self.logger.critical("Kerberos authentication FAILED after {0} attempts. HDFS commands will fail. \n>> ACTUAL ERROR: {1}".format(max_retries, error_msg))
                    raise Exception("Kerberos Authentication Failed: {0}".format(error_msg))

    def run(self):
        # 1. Start Dashboard Monitor
        monitor = MonitorThread(self.tracker)
        monitor.start()

        try:
            # 2. Phase 1: Generate Script
            self.generator.generate(self.output_sh)
            self.tracker.update_action("Script Generation Completed.")

            # 3. Phase 2: Execute (if not dry-run)
            if self.config.args.dry_run:
                self.tracker.update_action("DRY-RUN mode. Execution skipped.")
                self.logger.info("Dry-Run mode active. Skipping Phase 2 Execution.")
            else:
                has_tasks = any(r['status'] in ['ADD', 'FORCE'] for r in self.tracker.results)
                if not has_tasks:
                    self.tracker.update_action("No expired tables found. Execution skipped.")
                    self.logger.info("No tables to delete. Skipping Phase 2 Execution.")
                else:
                    self.tracker.update_action("Executing shell script (HDFS Deletion)...")
                    self.executor.execute(self.output_sh)

        except KeyboardInterrupt:
            self.logger.warning("Job Interrupted by User!")
        except Exception as e:
            self.logger.error("Job Aborted due to fatal error.", exc_info=True)
        finally:
            # 4. Stop Monitor and Print Summary
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.logger)
            self.logger.info("Job Finished. Log File: {0}".format(self.log_path))

if __name__ == "__main__":
    try:
        job = HousekeepingJob()
        job.run()
    except Exception as e:
        print("FATAL INIT ERROR: {0}".format(e))
        sys.exit(1)