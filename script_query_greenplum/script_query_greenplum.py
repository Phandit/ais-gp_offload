#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import csv
import logging
import subprocess
import shutil
import time
import argparse
import threading
import Queue
import re
import json
from datetime import datetime

# ==============================================================================
# 1. Utilities: Logger & ProcessTracker
# ==============================================================================

class ProcessTracker(object):
    def __init__(self, logger):
        self.logger = logger
        self.lock = threading.Lock()
        self.results = []
        self.worker_status = {}
        self.total_task = 0
        self.completed_task = 0
        self.start_time = time.time()

    def set_total_tasks(self, total):
        self.total_task = total

    def update_worker_status(self, worker_name, status):
        self.worker_status[worker_name] = status

    def add_result(self, table_name, status, message="-"):
        with self.lock:
            self.results.append({
                'table': table_name,
                'status': status,
                'message': str(message).replace('\n', ' ')
            })
            self.completed_task += 1

    def get_progress(self):
        with self.lock:
            return self.completed_task, self.total_task
    
    def log_step(self, step_name, duration):
        self.logger.info("Step: {0} | Duration: {1:.2f}s".format(step_name, duration))

    def print_summary(self, log_path):
        self.logger.info("="*80)
        self.logger.info("TABLE EXECUTION SUMMARY")
        self.logger.info("="*80)

        success_count = 0
        warning_count = 0
        failed_count = 0
        skipped_count = 0

        if not self.results:
            self.logger.info("No tables processed.")
        else:
            h_table = "Table Name"
            h_status = "Status"
            h_msg = "Error / Remark"

            max_w_table = len(h_table)
            max_w_status = len(h_status)

            for r in self.results:
                if len(r['table']) > max_w_table: max_w_table = len(r['table'])
                if len(r['status']) > max_w_status: max_w_status = len(r['status'])

                if r['status'] == 'SUCCESS': success_count += 1
                elif r['status'] == 'WARNING': warning_count += 1
                elif r['status'] == 'FAILED': failed_count += 1
                elif r['status'] == 'SKIPPED': skipped_count +=1
                       
            
            w_table = max_w_table + 2
            w_status = max_w_status + 2

            row_fmt = "{0:<{wt}} | {1:<{ws}} | {2}"

            header_line = row_fmt.format(h_table, h_status, h_msg, wt=w_table, ws=w_status)
            sep_line = "-" * len(header_line)
            if len(sep_line) < 50: sep_line = "-" * 50

            self.logger.info(sep_line)
            self.logger.info(header_line)
            self.logger.info(sep_line)

            for r in self.results:
                self.logger.info(row_fmt.format(r['table'], r['status'], r['message'], wt=w_table, ws=w_status))
            
            self.logger.info(sep_line)
            self.logger.info("Total: {0} | Success: {1} | Warning: {2} | Failed: {3} | Skipped: {4}".format(
                len(self.results), success_count, warning_count, failed_count, skipped_count
            ))
            self.logger.info("Total Execution Time: {0:.2f}s".format(time.time() - self.start_time))

        # Print Summary to Console (Last view)
        print("\n" + "="*80)
        print("FINAL SUMMARY REPORT")
        print("="*80)
        print("Total: {0}".format(len(self.results)))
        print("Success: {0}".format(success_count))
        print("Warning: {0}".format(warning_count))
        print("Failed:  {0}".format(failed_count))
        print("Skipped: {0}".format(skipped_count))
        print("Log File: {0}".format(log_path))
        print("="*80)

def setup_logging(log_dir, log_name="app", date_folder=None, timestamp=None):
    if date_folder:
        log_dir = os.path.join(log_dir, date_folder)

    if not os.path.exists(log_dir):
        try:
            os.makedirs(log_dir)
        except OSError as e:
            print("WARNING: Could not create log directory '{0}'. Using current directory. Error: {1}".format(log_dir, e))
            log_dir = '.'

    log_file = os.path.join(log_dir, "{0}_{1}.log".format(log_name, timestamp))

    logger = logging.getLogger("GreenplumBatch")
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger, log_file

def peek_env_config(env_path, key_to_find):
    value = None
    try:
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue
                    if '=' in line:
                        k, v = line.split('=', 1)
                        if k.strip() == key_to_find:
                            value = v.strip()
    except Exception:
        pass
    return value

# ==============================================================================
# 2. Configuration Class
# ==============================================================================

class Config(object):
    def __init__(self, env_config_path, list_file_path, cli_tables, logger, date_folder = None, main_path=None):
        self.logger = logger
        
        # 1. Load Environment Config
        self.logger.info("Loading environment config: {0}".format(env_config_path))
        self.local_temp_dir = os.path.join(main_path, 'temp')
        self.nas_dest_base = os.path.join(main_path, 'output')
        self.log_dir = os.path.join(main_path, 'log')
        self.metadata_base_dir = None
        self.config_master_file_path = None
        self.mapping_file_path = None

        # Default Number Configurations
        self.env_params = {
            'default_numeric_p': 38, 'default_numeric_s': 10,
            'cast_real_p': 24, 'cast_real_s': 6,
            'cast_double_p': 38, 'cast_double_s': 15,
            'round_numeric': 10, 'round_real': 5, 'round_double': 14
        }
        
        try:
            with open(env_config_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'): continue

                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()

                        if key == 'local_temp_dir': self.local_temp_dir = value
                        elif key == 'nas_destination': self.nas_dest_base = value
                        elif key == 'log_dir': self.log_dir = value
                        elif key == 'metadata_base_dir': self.metadata_base_dir = value
                        elif key == 'config_master_file_path': self.config_master_file_path = value
                        elif key == 'mapping_file_path': self.mapping_file_path = value
                        elif key in self.env_params:
                            self.env_params[key] = int(value)
                
            # Create temp dir if not exists
            if self.local_temp_dir:
                self.local_temp_dir = os.path.join(self.local_temp_dir, date_folder)
                if not os.path.exists(self.local_temp_dir):
                    os.makedirs(self.local_temp_dir)

            if self.nas_dest_base:
                self.nas_dest_base = os.path.join(self.nas_dest_base, date_folder)
            
            self.logger.info("Resolved local_temp_dir: {0}".format(self.local_temp_dir))
            self.logger.info("Resolved nas_dest_base: {0}".format(self.nas_dest_base))
            self.logger.info("Resolved log_dir: {0}".format(self.log_dir))
            self.logger.info("Resolved metadata_base_dir: {0}".format(self.metadata_base_dir))
            self.logger.info("Resolved mapping_file_path: {0}".format(self.mapping_file_path))

        except Exception as e:
            self.logger.error("Failed to load environment config: {0}".format(e))
            raise

        self.type_mapping = {"SUM_MIN_MAX": [], "MIN_MAX": [], "MD5_MIN_MAX": []}
        if self.mapping_file_path and os.path.exists(self.mapping_file_path):
            self.logger.info("Loading Data Type Mapping from JSON: {0}".format(self.mapping_file_path))
            try:
                with open(self.mapping_file_path, 'r') as f:
                    self.type_mapping = json.load(f)
            except Exception as e:
                self.logger.error("Failed to parse mapping file: {0}. Using empty map.".format(e))
        else:
            self.logger.warning("Mapping file not found or path not defined: {0}".format(self.mapping_file_path))

        # 2. Load Master Config (Lookup Dictionary)
        # Key: (db_name, schema, table) -> Value: row dict
        self.master_data = {}
        self.logger.info("Loading master config: {0}".format(self.config_master_file_path))
        try:
            with open(self.config_master_file_path, 'r') as f:
                reader = csv.reader(f, delimiter='|')
                for line in reader:
                    # Format: DB | SCHEMA | table | manual_num_col | manual_thai_col
                    if len(line) < 5: continue
                    db, sch, tbl, m_num, m_thai = [x.strip() for x in line[:5]]
                    key = (db, sch, tbl)
                    self.master_data[key] = {
                        'manual_num': [x.strip() for x in m_num.split(',') if x.strip().lower() != 'none' and x.strip()],
                        'manual_thai': [x.strip() for x in m_thai.split(',') if x.strip().lower() != 'none' and x.strip()]
                    }
            self.logger.info("Loaded {0} tables from master config.".format(len(self.master_data)))
        except Exception as e:
            self.logger.error("Failed to load master config: {0}".format(e))
            raise

        # 3. Determine Execution List
        self.execution_list = []
        if cli_tables:
            # Case 1: Use CLI Arguments
            self.logger.info("Using CLI arguments for table list.")
            # Expect format: DB|Schema.Table,DB2|Schema.Table
            tables = cli_tables.split(',')
            for t in tables:
                try:
                    # Parse DB|Schema.Table
                    db_part, tbl_part = t.split('|')
                    sch_part, real_tbl = tbl_part.split('.')
                    self.execution_list.append({
                        'db': db_part.strip(),
                        'schema': sch_part.strip(),
                        'table': real_tbl.strip()
                    })
                except ValueError:
                    self.logger.error("Invalid format in argument: {0}. Expected DB|Schema.Table".format(t))
        else:
            # Case 2: Use List File
            self.logger.info("Using list file: {0}".format(list_file_path))
            try:
                with open(list_file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'): continue
                        try:
                            # Format: DB|Schema.Table
                            db_part, tbl_part = line.split('|')
                            sch_part, real_tbl = tbl_part.split('.')
                            self.execution_list.append({
                                'db': db_part.strip(),
                                'schema': sch_part.strip(),
                                'table': real_tbl.strip()
                            })
                        except ValueError:
                            self.logger.warning("Skipping invalid line in list file: {0}".format(line))
            except Exception as e:
                self.logger.error("Failed to load list file: {0}".format(e))

# ==============================================================================
# 3. Handler Classes
# ==============================================================================

class QueryBuilder(object):
    def __init__(self, temp_dir, env_params, logger, global_ts):
        self.temp_dir = temp_dir
        self.env_params = env_params
        self.logger = logger
        self.global_ts = global_ts

    def _quote_json_val(self, sql_expr):
        return "COALESCE('\"' || ({0})::text || '\"', 'null')".format(sql_expr)
    
    def _build_num_expr(self, agg_func, col_expr, gp_type):
        gp_base = gp_type.split('(')[0].strip().lower()
        
        if gp_base == 'numeric' and '(' not in gp_type:
            p = self.env_params['default_numeric_p']
            s = self.env_params['default_numeric_s']
            r = self.env_params['round_numeric']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        elif gp_base == 'double precision':
            p = self.env_params['cast_double_p']
            s = self.env_params['cast_double_s']
            r = self.env_params['round_double']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        elif gp_base == 'real':
            p = self.env_params['cast_real_p']
            s = self.env_params['cast_real_s']
            r = self.env_params['round_real']
            return "ROUND({0}(({1})::numeric({2},{3})), {4})".format(agg_func, col_expr, p, s, r)
        else:
            return "{0}({1})".format(agg_func, col_expr)
        
    def build_json_query(self, db, schema, table, categorized_cols, insert_logic_dict):
        try:
            full_table_name = "{0}.{1}.{2}".format(db, schema, table)
            metric_fragments = []

            # Combine unique columns per solution category
            all_num_cols = set(categorized_cols['SUM_MIN_MAX'] + categorized_cols['MANUAL_NUM'])
            all_date_cols = set(categorized_cols['MIN_MAX'])
            all_cpx_cols = set(categorized_cols['MD5_MIN_MAX'] + categorized_cols['MANUAL_THAI'])

            num_fragments = []
            date_fragments = []
            cpx_fragments = []

            # 1. NUMBER Solution
            for col, gp_type in categorized_cols['TYPE_MAP'].items():
                if col in all_num_cols:
                    base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                    sum_expr = self._build_num_expr('SUM', base_expr, gp_type)
                    min_expr = self._build_num_expr('MIN', base_expr, gp_type)
                    max_expr = self._build_num_expr('MAX', base_expr, gp_type)
                    
                    json_parts = [
                        "'\"sum\": ' || {0}".format(self._quote_json_val(sum_expr)),
                        "'\"min\": ' || {0}".format(self._quote_json_val(min_expr)),
                        "'\"max\": ' || {0}".format(self._quote_json_val(max_expr))
                    ]
                    fragment = "'\"{0}\": {{' || {1} || '}}'".format(col, " || ', ' || ".join(json_parts))
                    num_fragments.append(fragment)
                    
                    # Remove from other sets if overlapped
                    all_date_cols.discard(col)
                    all_cpx_cols.discard(col)

            # 2. DATE Solution
            for col in all_date_cols:
                base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                min_expr = "MIN({0})::text".format(base_expr)
                max_expr = "MAX({0})::text".format(base_expr)
                
                json_parts = [
                    "'\"min\": ' || {0}".format(self._quote_json_val(min_expr)),
                    "'\"max\": ' || {0}".format(self._quote_json_val(max_expr))
                ]
                fragment = "'\"{0}\": {{' || {1} || '}}'".format(col, " || ', ' || ".join(json_parts))
                date_fragments.append(fragment)
                all_cpx_cols.discard(col)

            # 3. COMPLEX / THAI Solution
            for col in all_cpx_cols:
                base_expr = insert_logic_dict.get(col, '"{0}"'.format(col))
                min_expr = "MIN(MD5(COALESCE(({0})::text, '')))".format(base_expr)
                max_expr = "MAX(MD5(COALESCE(({0})::text, '')))".format(base_expr)
                
                json_parts = [
                    "'\"min_md5\": ' || {0}".format(self._quote_json_val(min_expr)),
                    "'\"max_md5\": ' || {0}".format(self._quote_json_val(max_expr))
                ]
                fragment = "'\"{0}\": {{' || {1} || '}}'".format(col, " || ', ' || ".join(json_parts))
                cpx_fragments.append(fragment)

            method_groups = []
            if num_fragments:
                method_groups.append("'\"SUM_MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(num_fragments)))
            if date_fragments:
                method_groups.append("'\"MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(date_fragments)))
            if cpx_fragments:
                method_groups.append("'\"MD5_MIN_MAX\": {{' || {0} || '}}'".format(" || ', ' || ".join(cpx_fragments)))

            # Assemble Final SQL
            metrics_sql = " || ', ' || ".join(method_groups) if method_groups else "''"
            
            # Construct single JSON object string
            sql = (
                "SELECT '{{' || "
                " '\"table\": \"{0}\", ' || "
                " '\"count\": ' || COUNT(*)::text || ', ' || "
                " '\"metrics\": {{' || {1} || '}}' || "
                " '}}' "
                "FROM {2}.{3};"
            ).format(full_table_name, metrics_sql, schema, table)

            filename = "query_{0}_{1}_{2}_{3}.sql".format(db, schema, table, self.global_ts)
            filepath = os.path.join(self.temp_dir, filename)

            with open(filepath, 'w') as f:
                f.write(sql)

            return sql
        
        except Exception as e:
            self.logger.error("Error building JSON query: {0}".format(e))
            raise

class ShellHandler(object):
    def __init__(self, logger):
        self.logger = logger

    def run_psql(self, sql, output_path, db_name=None):
        cmd = ['psql', '-A', '-t', '-c', sql, '-o', output_path]

        if db_name: cmd.extend(['-d', db_name])

        self.logger.info("Executing PSQL... (DB: {0}) -> Output: {1}".format(db_name or 'Default', output_path))
        try:
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            if process.returncode != 0:
                raise RuntimeError("PSQL execution failed. RC: {0}. Error: {1}".format(process.returncode, stderr))
            if not (os.path.exists(output_path) and os.path.getsize(output_path) > 0):
                self.logger.warning("PSQL Executed but output file is missing or 0 bytes: {0}".format(output_path))
        except Exception as e:
            raise

class FileHandler(object):
    def __init__(self, logger):
        self.logger = logger
    def copy_to_nas(self, src, dest_dir):
        if not os.path.exists(dest_dir):
            try:
                os.makedirs(dest_dir)
            except OSError:
                pass
        self.logger.info("Copying file from {0} to NAS: {1}".format(src, dest_dir))
        shutil.copy2(src, dest_dir)

# ==============================================================================
# 3. Parallel Workers & Monitor
# ==============================================================================

class Worker(threading.Thread):
    def __init__(self, thread_id, job_queue, config, builder, shell, file_h, tracker, logger, global_ts):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.name = "Worker-{0:02d}".format(thread_id)
        self.queue = job_queue
        self.config = config
        self.builder = builder
        self.shell = shell
        self.file_h = file_h
        self.tracker = tracker
        self.logger = logger
        self.global_ts = global_ts
        self.daemon = True

    def _get_latest_metadata(self, db_name, table_name):
        """Scans metadata_base_dir for the latest data_type and insert_logic files"""
        if not self.config.metadata_base_dir or not os.path.exists(self.config.metadata_base_dir):
            return None, None
        
        target_dir = os.path.join(self.config.metadata_base_dir, db_name)

        if not os.path.exists(target_dir):
            return None, None
            
        matches_dt = []
        matches_il = []
        
        # Walk through directories to find the most recent file
        for root, _, files in os.walk(target_dir):
            for file in files:
                if file.endswith("_data_type.txt") and table_name in file:
                    matches_dt.append(os.path.join(root, file))
                elif file.endswith("_insert_logic.txt") and table_name in file:
                    matches_il.append(os.path.join(root, file))
                    
        latest_dt = sorted(matches_dt)[-1] if matches_dt else None
        latest_il = sorted(matches_il)[-1] if matches_il else None
        return latest_dt, latest_il

    def run(self):
        while True:
            try:
                task = self.queue.get(block=True, timeout=2)
            except Queue.Empty:
                self.tracker.update_worker_status(self.name, "[IDLE] Finished")
                break

            try:
                db = task['db']
                schema = task['schema']
                table = task['table']

                full_name = "{0}.{1}.{2}".format(db, schema, table)
                short_name = "{0}.{1}".format(schema, table)

                # --- RETRY LOOP ---
                max_retries = 1
                for attempt in range(max_retries):
                    try:
                        self.tracker.update_worker_status(self.name, "[BUSY] {0} (Try {1}/{2})".format(short_name, attempt+1, max_retries))
                        start_t = time.time()

                        self.logger.info("Worker {0} started processing table: {1} (Attempt {2})".format(self.name, full_name, attempt+1))

                        # 1. Config Lookup
                        key = (db, schema, table)
                        master_info = self.config.master_data.get(key, {'manual_num': [], 'manual_thai': []})

                        # Dynamic Metadata Discovery ---
                        dt_file, il_file = self._get_latest_metadata(db, table)
                        missing_flag = False if dt_file else True

                        # --- Parse Insert Logic ---
                        insert_logic_dict = {}
                        if il_file:
                            try:
                                with open(il_file, 'r') as f:
                                    content = f.read()
                                    current_col = None
                                    for line in content.splitlines():
                                        if not line.strip(): continue
                                        if line == "gp_column_nm;insert_logic": continue # skip Header
                                        
                                        # match "column_name;logic"
                                        match = re.match(r'^([a-zA-Z0-9_]+);(.*)', line)
                                        if match:
                                            current_col = match.group(1)
                                            insert_logic_dict[current_col] = match.group(2)
                                        elif current_col:
                                            insert_logic_dict[current_col] += " \n" + line

                                for col in insert_logic_dict:
                                    logic = insert_logic_dict[col]
                                    logic = logic.replace('\\n', ' ').replace('\n', ' ')
                                    logic = re.sub(r'(?i)\s+AS\s+"?[a-zA-Z0-9_]+"?(?:\s*)$', '', logic)
                                    insert_logic_dict[col] = logic.strip()
                            except Exception as e:
                                self.logger.warning("Worker {0} error reading logic file {1}: {2}".format(self.name, il_file, e))

                        cat_cols = {'SUM_MIN_MAX': [], 'MIN_MAX': [], 'MD5_MIN_MAX': [], 'TYPE_MAP': {}, 
                                    'MANUAL_NUM': master_info['manual_num'], 
                                    'MANUAL_THAI': master_info['manual_thai']}
                        if dt_file:
                            try:
                                with open(dt_file, 'r') as f:
                                    reader = csv.DictReader(f, delimiter='|')
                                    for row in reader:
                                        col_nm = row.get('gp_column_nm', '').strip()
                                        gp_dt = row.get('gp_datatype', '').strip()
                                        ext_dt = row.get('ext_datatype', '').strip().lower()

                                        if col_nm and gp_dt:
                                            cat_cols['TYPE_MAP'][col_nm] = gp_dt
                                            if any(t in ext_dt for t in ['text', 'varchar', 'character', 'string']):
                                                gp_base = gp_dt.split('(')[0].strip().lower()

                                                if gp_base in self.config.type_mapping.get("SUM_MIN_MAX", []):
                                                    cat_cols['SUM_MIN_MAX'].append(col_nm)
                                                elif gp_base in self.config.type_mapping.get("MIN_MAX", []):
                                                    cat_cols['MIN_MAX'].append(col_nm)
                                                elif gp_base in self.config.type_mapping.get("MD5_MIN_MAX", []):
                                                    cat_cols['MD5_MIN_MAX'].append(col_nm)
                            except Exception as e:
                                self.logger.warning("Worker {0} error reading data type file {1}: {2}".format(self.name, dt_file, e))
                                missing_flag = True

                        # --- Call JSON builder and save as .json file ---
                        sql = self.builder.build_json_query(db, schema, table, cat_cols, insert_logic_dict)
                        output_filename = "{0}_{1}_{2}_{3}.json".format(db, schema, table, self.global_ts)
                        local_path = os.path.join(self.config.local_temp_dir, output_filename)

                        self.shell.run_psql(sql, local_path, db)
                        final_nas_dest = os.path.join(self.config.nas_dest_base, db, schema)
                        self.file_h.copy_to_nas(local_path, final_nas_dest)

                        # Complete Task
                        if missing_flag:
                            self.tracker.add_result(full_name, "WARNING", "Count-only. Metadata file missing.")
                        else:
                            self.tracker.add_result(full_name, "SUCCESS", "-")

                        self.tracker.log_step(full_name, time.time() - start_t)
                        break # break retry loop on success

                    except Exception as e:
                        safe_err = repr(e)
                        if attempt < max_retries - 1:
                            time.sleep(3)
                        else:
                            self.tracker.add_result(full_name, "FAILED", str(safe_err))

            except Exception as outer_e:
                self.logger.error("Worker {0} FATAL CRASH. Error: {1}".format(self.name, repr(outer_e)))
                self.tracker.add_result(str(task.get('table', 'UNKNOWN')), "CRASHED", repr(outer_e))
            finally:
                self.queue.task_done()

class MonitorThread(threading.Thread):
    def __init__(self, tracker, num_workers, log_path):
        threading.Thread.__init__(self)
        self.tracker = tracker
        self.num_workers = num_workers
        self.log_path = log_path
        self.stop_event = threading.Event()
        self.daemon = True
        self.first_print = True

    def stop(self):
        self.stop_event.set()

    def run(self):
        while not self.stop_event.is_set():
            self.print_dashboard()
            time.sleep(1)
        self.print_dashboard()

    def print_dashboard(self):
        comp, total = self.tracker.get_progress()
        pct = 100.0 * comp / total if total > 0 else 0
        elapsed = time.time() - self.tracker.start_time

        lines = []
        lines.append("============================================================")
        lines.append(" GREENPLUM EXPORT MONITOR (Python 2.7 Parallel) ")
        lines.append("============================================================")
        lines.append(" Progress: {0}/{1} ({2:.2f}%)".format(comp, total, pct))
        lines.append(" Elapsed : {0:.0f}s".format(elapsed))
        lines.append("-" * 60)

        workers = sorted(self.tracker.worker_status.keys())
        for w_name in workers:
            status = self.tracker.worker_status.get(w_name, "Initializing...")
            line_str = " {0} : {1}".format(w_name, status)
            lines.append(line_str[:79]) 
        
        lines.append("-" * 60)
        lines.append(" Log File: {0}".format(self.log_path))
        lines.append(" Press Ctrl+C to abort.")

        if not self.first_print:
            sys.stdout.write('\033[F' * len(lines))
        else:
            self.first_print = False

        output = "\n".join([line + "\033[K" for line in lines]) + "\n"
        
        sys.stdout.write(output)
        sys.stdout.flush()

# ==============================================================================
# 4. Main Job Class
# ==============================================================================
class GreenplumExportJob(object):
    def __init__(self, args, logger, log_path, global_date_folder, global_ts, main_path):
        self.args = args
        self.logger = logger
        self.log_path = log_path
        self.global_ts = global_ts
        self.tracker = ProcessTracker(logger)

        # Init Helpers
        self.config = Config(args.env, args.list, args.table_name, logger, global_date_folder, main_path)
        self.builder = QueryBuilder(self.config.local_temp_dir, self.config.env_params, logger, self.global_ts)
        self.shell = ShellHandler(logger)
        self.file_h = FileHandler(logger)

        # Setup Queue
        self.job_queue = Queue.Queue()
        for task in self.config.execution_list:
            self.job_queue.put(task)

        self.tracker.set_total_tasks(len(self.config.execution_list))
        self.logger.info("Loaded {0} tasks into queue.".format(len(self.config.execution_list)))

    def run(self):
        num_workers = int(self.args.concurrency)
        self.logger.info("Starting {0} workers...".format(num_workers))

        workers = []
        # Create & Start Workers
        for i in range(num_workers):
            w = Worker(i+1, self.job_queue, self.config, self.builder, self.shell, self.file_h, self.tracker, self.logger, self.global_ts)
            workers.append(w)
            w.start()

        # Start Monitor
        monitor = MonitorThread(self.tracker, num_workers, self.log_path)
        monitor.start()

        # Wait for Queue to be empty
        try:
            while self.tracker.completed_task < self.tracker.total_task:
                if not any(w.is_alive() for w in workers):
                    self.logger.error("All workers died unexpectedly! Aborting wait loop.")
                    break
                time.sleep(1)
            for w in workers:
                w.join()
        except KeyboardInterrupt:
            sys.stdout.write("\n\n>>> KEYBOARD INTERRUPT DETECTED! ABORTING SCRIPT... <<<\n\n")
            sys.stdout.flush()
            self.logger.warning("Keyboard Interrupt! User aborted the script.")
        finally:
            monitor.stop()
            monitor.join()
            self.tracker.print_summary(self.log_path)

if __name__ == "__main__":
    current_script_dir = os.path.dirname(os.path.abspath(__file__))
    main_path = os.path.dirname(current_script_dir)

    parser = argparse.ArgumentParser(description='Greenplum Data Export Tool')
    parser.add_argument('--env', default='env_config.txt', help='Name of env config file')
    parser.add_argument('--master', default='config_master.txt', help='Name of master config file')
    parser.add_argument('--list', default='list_table.txt', help='Name of list of tables file')
    parser.add_argument('--table_name', help='Optional: Specific tables to run (DB|Schema.Table)')
    parser.add_argument('--concurrency', default=4, type=int, help='Number of parallel workers (Default: 4)')

    args = parser.parse_args()

    def resolve_config_path(input_path, base_dir):
        if os.path.isabs(input_path):
            return input_path
        return os.path.join(base_dir, 'config', input_path)

    args.env = resolve_config_path(args.env, main_path)
    args.master = resolve_config_path(args.master, main_path)
    args.list = resolve_config_path(args.list, main_path)

    run_datetime = datetime.now()
    global_date_folder = run_datetime.strftime("%Y%m%d")
    global_ts = run_datetime.strftime("%Y%m%d_%H%M%S")

    configured_log_dir = peek_env_config(args.env, 'log_dir')
    final_log_dir = configured_log_dir if configured_log_dir else os.path.join(main_path, 'log')

    logger, log_path = setup_logging(final_log_dir, 'reconcile_query_greenplum', global_date_folder, global_ts)
    
    logger.info("============================================================")
    logger.info("Started with concurrency: {0}".format(args.concurrency))
    logger.info("Resolved env config path: {0}".format(args.env))
    logger.info("Resolved master config path: {0}".format(args.master))
    logger.info("Resolved list file path: {0}".format(args.list))
    logger.info("============================================================")

    try:
        job = GreenplumExportJob(args, logger, log_path, global_date_folder, global_ts, main_path)
        job.run()
    except Exception as e:
        logger.critical("Job aborted due to critical error: {0}".format(e))
        sys.exit(1)