# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

import copy
import itertools
import json
import logging
import math
import os
import re
import socket
import stat
import time
import traceback
from collections import defaultdict
from datetime import timedelta
from io import BytesIO
from pathlib import Path

import markdown
import pendulum
import sqlalchemy as sqla
from flask import (Markup, Response, flash, g, jsonify, make_response,
                   redirect, render_template, request, send_file, url_for)
from flask._compat import PY2
from flask_appbuilder import BaseView, ModelView, expose, has_access
from flask_appbuilder.actions import action
from flask_appbuilder.models.sqla.filters import BaseFilter
from flask_babel import lazy_gettext
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import and_, desc, func, or_, union_all
from wtforms import SelectField, validators

import airflow
from airflow import configuration as conf
from airflow import jobs, models, settings
from airflow._vendor import nvd3
from airflow.api.common.experimental.mark_tasks import (
    set_dag_run_state_to_failed, set_dag_run_state_to_success)
from airflow.configuration import AIRFLOW_HOME
from airflow.models import (Connection, DagModel, DagRun, Log, SlaMiss,
                            TaskFail, XCom, errors)
from airflow.ti_deps.dep_context import QUEUE_DEPS, SCHEDULER_DEPS, DepContext
from airflow.utils import timezone
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.utils.db import provide_session
from airflow.utils.helpers import alchemy_to_dict, render_log_filename
from airflow.utils.state import State
from airflow.www_rbac import utils as wwwutils
from airflow.www_rbac.app import app, appbuilder
from airflow.www_rbac.decorators import action_logging, gzipped, has_dag_access
from airflow.www_rbac.forms import (ConnectionForm, DagRunForm, DateTimeForm,
                                    DateTimeWithNumRunsForm,
                                    DateTimeWithNumRunsWithDagRunsForm)
from airflow.www_rbac.widgets import AirflowModelListWidget

PAGE_SIZE = conf.getint('webserver', 'page_size')
if os.environ.get('SKIP_DAGS_PARSING') != 'True':
    dagbag = models.DagBag(settings.DAGS_FOLDER)
else:
    dagbag = models.DagBag(os.devnull, include_examples=False)


def get_date_time_num_runs_dag_runs_form_data(request, session, dag):
    dttm = request.args.get('execution_date')
    if dttm:
        dttm = pendulum.parse(dttm)
    else:
        dttm = dag.latest_execution_date or timezone.utcnow()

    base_date = request.args.get('base_date')
    if base_date:
        base_date = timezone.parse(base_date)
    else:
        # The DateTimeField widget truncates milliseconds and would loose
        # the first dag run. Round to next second.
        base_date = (dttm + timedelta(seconds=1)).replace(microsecond=0)

    default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
    num_runs = request.args.get('num_runs')
    num_runs = int(num_runs) if num_runs else default_dag_run

    DR = models.DagRun
    drs = (
        session.query(DR)
        .filter(
            DR.dag_id == dag.dag_id,
            DR.execution_date <= base_date)
        .order_by(desc(DR.execution_date))
        .limit(num_runs)
        .all()
    )
    dr_choices = []
    dr_state = None
    for dr in drs:
        dr_choices.append((dr.execution_date.isoformat(), dr.run_id))
        if dttm == dr.execution_date:
            dr_state = dr.state

    # Happens if base_date was changed and the selected dag run is not in result
    if not dr_state and drs:
        dr = drs[0]
        dttm = dr.execution_date
        dr_state = dr.state

    return {
        'dttm': dttm,
        'base_date': base_date,
        'num_runs': num_runs,
        'execution_date': dttm.isoformat(),
        'dr_choices': dr_choices,
        'dr_state': dr_state,
    }


######################################################################################
#                                    BaseViews
######################################################################################

@app.errorhandler(404)
def circles(error):
    return render_template(
        'airflow/circles.html', hostname=socket.getfqdn()), 404


@app.errorhandler(500)
def show_traceback(error):
    from airflow.utils import asciiart as ascii_
    return render_template(
        'airflow/traceback.html',
        hostname=socket.getfqdn(),
        nukular=ascii_.nukular,
        info=traceback.format_exc()), 500


class AirflowBaseView(BaseView):
    route_base = ''
    @provide_session
    def audit_logging(event_name, extra, source_ip, session=None):
        if g.user.is_anonymous:
            user = 'anonymous'
        else:
            user = g.user.username

        log = Log(
            event=event_name,
            task_instance=None,
            owner=user,
            extra=extra,
            task_id=None,
            dag_id=None,
            source_ip=source_ip)

        session.add(log)

    def convert_size(size_bytes):
        # TODO: this method is buggy, change it.
        # to get size of file
        if size_bytes == 0:
            return "0B"
        size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return "%s %s" % (s, size_name[i])

    def get_details(dir_path, file_extension):
        ''' Takes the path and file extension, returns size and last modified time of files
        inside the path.
        '''

        # NOTE: This method use `os.walk`. We may want to use `os.listdir()` instead.
        file_data = {}
        for r, d, f in os.walk(dir_path):
            for file_name in f:
                if file_name.endswith(file_extension):
                    filePath = os.path.join(dir_path, file_name)
                    if(os.path.exists(filePath)):
                        fileStatsObj = os.stat(filePath)
                        modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])  # get last modified time
                        size_bytes = os.stat(filePath).st_size
                        if size_bytes == 0:
                            size = "0B"
                        else:
                            size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
                            i = int(math.floor(math.log(size_bytes, 1024)))
                            p = math.pow(1024, i)
                            s = round(size_bytes / p, 2)
                            size = "%s %s" % (s, size_name[i])
                        temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size}
                        file_data[file_name] = temp_dict
        return file_data

    def get_len_jar(file_data):
        '''To get the number of jar files'''
        len_jar = 0
        for file in file_data:
            if file.endswith(".jar"):
                len_jar = len_jar + 1
        return len_jar

    def get_len_py(file_data):
        '''to get the number of py files'''
        len_py = 0
        for file in file_data:
            if file.endswith(".py") or file.endswith(".egg") or file.endswith(".zip"):
                len_py = len_py + 1
        return len_py


class Airflow(AirflowBaseView):
    @expose('/health')
    @provide_session
    def health(self, session=None):
        """
        An endpoint helping check the health status of the Airflow instance,
        including metadatabase and scheduler.
        """

        BJ = jobs.BaseJob
        payload = {}
        scheduler_health_check_threshold = timedelta(seconds=conf.getint('scheduler',
                                                                         'scheduler_health_check_threshold'
                                                                         ))

        latest_scheduler_heartbeat = None
        payload['metadatabase'] = {'status': 'healthy'}
        try:
            latest_scheduler_heartbeat = session.query(func.max(BJ.latest_heartbeat)). \
                filter(BJ.state == 'running', BJ.job_type == 'SchedulerJob'). \
                scalar()
        except Exception:
            payload['metadatabase']['status'] = 'unhealthy'

        if not latest_scheduler_heartbeat:
            scheduler_status = 'unhealthy'
        else:
            if timezone.utcnow() - latest_scheduler_heartbeat <= scheduler_health_check_threshold:
                scheduler_status = 'healthy'
            else:
                scheduler_status = 'unhealthy'

        payload['scheduler'] = {'status': scheduler_status,
                                'latest_scheduler_heartbeat': str(latest_scheduler_heartbeat)}

        return wwwutils.json_response(payload)

    @expose('/home')
    @has_access
    @provide_session
    def index(self, session=None):
        DM = models.DagModel

        hide_paused_dags_by_default = conf.getboolean('webserver',
                                                      'hide_paused_dags_by_default')
        show_paused_arg = request.args.get('showPaused', 'None')

        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        def get_int_arg(value, default=0):
            try:
                return int(value)
            except ValueError:
                return default

        arg_current_page = request.args.get('page', '0')
        arg_search_query = request.args.get('search', None)

        dags_per_page = PAGE_SIZE
        current_page = get_int_arg(arg_current_page, default=0)

        if show_paused_arg.strip().lower() == 'false':
            hide_paused = True
        elif show_paused_arg.strip().lower() == 'true':
            hide_paused = False
        else:
            hide_paused = hide_paused_dags_by_default

        # read orm_dags from the db
        query = session.query(DM).filter(
            ~DM.is_subdag, DM.is_active
        )

        # optionally filter out "paused" dags
        if hide_paused:
            query = query.filter(~DM.is_paused)

        if arg_search_query:
            query = query.filter(
                DagModel.dag_id.ilike('%' + arg_search_query + '%') |
                DagModel.owners.ilike('%' + arg_search_query + '%')
            )

        import_errors = session.query(errors.ImportError).all()
        for ie in import_errors:
            flash(
                "Broken DAG: [{ie.filename}] {ie.stacktrace}".format(ie=ie),
                "error")

        from airflow.plugins_manager import import_errors as plugin_import_errors
        for filename, stacktrace in plugin_import_errors.items():
            flash(
                "Broken plugin: [{filename}] {stacktrace}".format(
                    stacktrace=stacktrace,
                    filename=filename),
                "error")

        # Get all the dag id the user could access
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' not in filter_dag_ids:
            query = query.filter(DM.dag_id.in_(filter_dag_ids))

        query = query.order_by(DM.dag_id)

        start = current_page * dags_per_page
        end = start + dags_per_page

        dags = query.offset(start).limit(dags_per_page).all()

        num_of_all_dags = query.count()
        num_of_pages = int(math.ceil(num_of_all_dags / float(dags_per_page)))

        auto_complete_data = set()
        for row in query.with_entities(DM.dag_id, DM.owners):
            auto_complete_data.add(row.dag_id)
            auto_complete_data.add(row.owners)
        user = None
        if "COUTURE_WORKFLOW_USER" in os.environ:
            user = os.environ['COUTURE_WORKFLOW_USER']

        return self.render_template(
            'airflow/dags.html',
            dags=dags,
            hide_paused=hide_paused,
            current_page=current_page,
            search_query=arg_search_query if arg_search_query else '',
            page_size=dags_per_page,
            num_of_pages=num_of_pages,
            num_dag_from=start + 1,
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(current_page, num_of_pages,
                                           search=arg_search_query,
                                           showPaused=not hide_paused),
            auto_complete_data=auto_complete_data,
            num_runs=num_runs,
            user=user)

    @expose('/dag_stats')
    @has_access
    @provide_session
    def dag_stats(self, session=None):
        dr = models.DagRun

        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        dag_state_stats = session.query(dr.dag_id, dr.state, sqla.func.count(dr.state)) \
            .group_by(dr.dag_id, dr.state)

        payload = {}
        if filter_dag_ids:
            if 'all_dags' not in filter_dag_ids:
                dag_state_stats = dag_state_stats.filter(dr.dag_id.in_(filter_dag_ids))
            data = {}
            for dag_id, state, count in dag_state_stats:
                if dag_id not in data:
                    data[dag_id] = {}
                data[dag_id][state] = count

            if 'all_dags' in filter_dag_ids:
                filter_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

            for dag_id in filter_dag_ids:
                if 'all_dags' in filter_dag_ids or dag_id in filter_dag_ids:
                    payload[dag_id] = []
                    for state in State.dag_states:
                        count = data.get(dag_id, {}).get(state, 0)
                        payload[dag_id].append({
                            'state': state,
                            'count': count,
                            'dag_id': dag_id,
                            'color': State.color(state)
                        })
        return wwwutils.json_response(payload)

    @expose('/task_stats')
    @has_access
    @provide_session
    def task_stats(self, session=None):
        TI = models.TaskInstance
        DagRun = models.DagRun
        Dag = models.DagModel

        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        payload = {}
        if not filter_dag_ids:
            return

        LastDagRun = (
            session.query(
                DagRun.dag_id,
                sqla.func.max(DagRun.execution_date).label('execution_date'))
                .join(Dag, Dag.dag_id == DagRun.dag_id)
                .filter(DagRun.state != State.RUNNING)
                .filter(Dag.is_active == True)  # noqa
                .group_by(DagRun.dag_id)
                .subquery('last_dag_run')
        )
        RunningDagRun = (
            session.query(DagRun.dag_id, DagRun.execution_date)
                .join(Dag, Dag.dag_id == DagRun.dag_id)
                .filter(DagRun.state == State.RUNNING)
                .filter(Dag.is_active == True)  # noqa
                .subquery('running_dag_run')
        )

        # Select all task_instances from active dag_runs.
        # If no dag_run is active, return task instances from most recent dag_run.
        LastTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
            .join(LastDagRun,
                  and_(LastDagRun.c.dag_id == TI.dag_id,
                       LastDagRun.c.execution_date == TI.execution_date))
        )
        RunningTI = (
            session.query(TI.dag_id.label('dag_id'), TI.state.label('state'))
            .join(RunningDagRun,
                  and_(RunningDagRun.c.dag_id == TI.dag_id,
                       RunningDagRun.c.execution_date == TI.execution_date))
        )

        UnionTI = union_all(LastTI, RunningTI).alias('union_ti')
        qry = (
            session.query(UnionTI.c.dag_id, UnionTI.c.state, sqla.func.count())
            .group_by(UnionTI.c.dag_id, UnionTI.c.state)
        )

        data = {}
        for dag_id, state, count in qry:
            if 'all_dags' in filter_dag_ids or dag_id in filter_dag_ids:
                if dag_id not in data:
                    data[dag_id] = {}
                data[dag_id][state] = count
        session.commit()

        if 'all_dags' in filter_dag_ids:
            filter_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]
        for dag_id in filter_dag_ids:
            if 'all_dags' in filter_dag_ids or dag_id in filter_dag_ids:
                payload[dag_id] = []
                for state in State.task_states:
                    count = data.get(dag_id, {}).get(state, 0)
                    payload[dag_id].append({
                        'state': state,
                        'count': count,
                        'dag_id': dag_id,
                        'color': State.color(state)
                    })
        return wwwutils.json_response(payload)

    @expose('/code')
    @has_dag_access(can_dag_read=True)
    @has_access
    @provide_session
    def code(self, session=None):
        dm = models.DagModel
        dag_id = request.args.get('dag_id')
        dag = session.query(dm).filter(dm.dag_id == dag_id).first()
        try:
            with wwwutils.open_maybe_zipped(dag.fileloc, 'r') as f:
                code = f.read()
            html_code = highlight(
                code, lexers.PythonLexer(), HtmlFormatter(linenos=True))
        except IOError as e:
            html_code = str(e)

        return self.render_template(
            'airflow/dag_code.html', html_code=html_code, dag=dag, title=dag_id,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'))

    @expose('/dag_details')
    @has_dag_access(can_dag_read=True)
    @has_access
    @provide_session
    def dag_details(self, session=None):
        dag_id = request.args.get('dag_id')
        dag_orm = DagModel.get_dagmodel(dag_id)
        # FIXME: items needed for this view should move to the database
        dag = dag_orm.get_dag()
        title = "DAG details"
        root = request.args.get('root', '')

        TI = models.TaskInstance
        states = (
            session.query(TI.state, sqla.func.count(TI.dag_id))
            .filter(TI.dag_id == dag_id)
            .group_by(TI.state)
            .all()
        )

        active_runs = models.DagRun.find(
            dag_id=dag_id,
            state=State.RUNNING,
            external_trigger=False
        )

        return self.render_template(
            'airflow/dag_details.html',
            dag=dag, title=title, root=root, states=states, State=State, active_runs=active_runs)

    @expose('/pickle_info')
    @has_access
    def pickle_info(self):
        d = {}
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        if not filter_dag_ids:
            return wwwutils.json_response({})
        dag_id = request.args.get('dag_id')
        dags = [dagbag.dags.get(dag_id)] if dag_id else dagbag.dags.values()
        for dag in dags:
            if 'all_dags' in filter_dag_ids or dag.dag_id in filter_dag_ids:
                if not dag.is_subdag:
                    d[dag.dag_id] = dag.pickle_info()
        return wwwutils.json_response(d)

    @expose('/rendered')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def rendered(self):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dag = dagbag.get_dag(dag_id)
        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.render_templates()
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.__class__.template_fields:
            content = getattr(task, template_field)
            if template_field in wwwutils.get_attr_renderer():
                html_dict[template_field] = \
                    wwwutils.get_attr_renderer()[template_field](content)
            else:
                html_dict[template_field] = (
                    "<pre><code>" + str(content) + "</pre></code>")

        return self.render_template(
            'airflow/ti_code.html',
            html_dict=html_dict,
            dag=dag,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            title=title)

    @expose('/get_logs_with_metadata')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def get_logs_with_metadata(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        if request.args.get('try_number') is not None:
            try_number = int(request.args.get('try_number'))
        else:
            try_number = None
        metadata = request.args.get('metadata')
        metadata = json.loads(metadata)
        response_format = request.args.get('format', 'json')

        # metadata may be null
        if not metadata:
            metadata = {}

        # Convert string datetime into actual datetime
        try:
            execution_date = timezone.parse(execution_date)
        except ValueError:
            error_message = (
                'Given execution date, {}, could not be identified '
                'as a date. Example date format: 2015-11-16T14:34:15+00:00'.format(
                    execution_date))
            response = jsonify({'error': error_message})
            response.status_code = 400

            return response

        logger = logging.getLogger('airflow.task')
        task_log_reader = conf.get('core', 'task_log_reader')
        handler = next((handler for handler in logger.handlers
                        if handler.name == task_log_reader), None)

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == dttm).first()
        try:
            if ti is None:
                logs = ["*** Task instance did not exist in the DB\n"]
                metadata['end_of_log'] = True
            else:
                dag = dagbag.get_dag(dag_id)
                ti.task = dag.get_task(ti.task_id)
                logs, metadatas = handler.read(ti, try_number, metadata=metadata)
                metadata = metadatas[0]

            if response_format == 'json':
                message = logs[0] if try_number is not None else logs
                return jsonify(message=message, metadata=metadata)

            file_obj = BytesIO(b'\n'.join(
                log.encode('utf-8') for log in logs
            ))
            filename_template = conf.get('core', 'LOG_FILENAME_TEMPLATE')
            attachment_filename = render_log_filename(
                ti=ti,
                try_number="all" if try_number is None else try_number,
                filename_template=filename_template)
            return send_file(file_obj, as_attachment=True,
                             attachment_filename=attachment_filename)
        except AttributeError as e:
            error_message = ["Task log handler {} does not support read logs.\n{}\n"
                             .format(task_log_reader, str(e))]
            metadata['end_of_log'] = True
            return jsonify(message=error_message, error=True, metadata=metadata)

    @expose('/log')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def log(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        dag = dagbag.get_dag(dag_id)

        ti = session.query(models.TaskInstance).filter(
            models.TaskInstance.dag_id == dag_id,
            models.TaskInstance.task_id == task_id,
            models.TaskInstance.execution_date == dttm).first()

        num_logs = 0
        if ti is not None:
            num_logs = ti.next_try_number - 1
            if ti.state == State.UP_FOR_RESCHEDULE:
                # Tasks in reschedule state decremented the try number
                num_logs += 1
        logs = [''] * num_logs
        root = request.args.get('root', '')
        return self.render_template(
            'airflow/ti_log.html',
            logs=logs, dag=dag, title="Log by attempts",
            dag_id=dag.dag_id, task_id=task_id,
            execution_date=execution_date, form=form,
            root=root)

    @expose('/task')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def task(self):
        TI = models.TaskInstance

        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dag = dagbag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect(url_for('Airflow.index'))
        task = copy.copy(dag.get_task(task_id))
        task.resolve_template_files()
        ti = TI(task=task, execution_date=dttm)
        ti.refresh_from_db()

        ti_attrs = []
        for attr_name in dir(ti):
            if not attr_name.startswith('_'):
                attr = getattr(ti, attr_name)
                if type(attr) != type(self.task):  # noqa
                    ti_attrs.append((attr_name, str(attr)))

        task_attrs = []
        for attr_name in dir(task):
            if not attr_name.startswith('_'):
                attr = getattr(task, attr_name)
                if type(attr) != type(self.task) and \
                    attr_name not in wwwutils.get_attr_renderer():  # noqa
                    task_attrs.append((attr_name, str(attr)))

        # Color coding the special attributes that are code
        special_attrs_rendered = {}
        for attr_name in wwwutils.get_attr_renderer():
            if hasattr(task, attr_name):
                source = getattr(task, attr_name)
                special_attrs_rendered[attr_name] = \
                    wwwutils.get_attr_renderer()[attr_name](source)

        no_failed_deps_result = [(
            "Unknown",
            "All dependencies are met but the task instance is not running. In most "
            "cases this just means that the task will probably be scheduled soon "
            "unless:<br/>\n- The scheduler is down or under heavy load<br/>\n{}\n"
            "<br/>\nIf this task instance does not start soon please contact your "
            "Airflow administrator for assistance.".format(
                "- This task instance already ran and had it's state changed manually "
                "(e.g. cleared in the UI)<br/>" if ti.state == State.NONE else ""))]

        # Use the scheduler's context to figure out which dependencies are not met
        dep_context = DepContext(SCHEDULER_DEPS)
        failed_dep_reasons = [(dep.dep_name, dep.reason) for dep in
                              ti.get_failed_dep_statuses(
                                  dep_context=dep_context)]

        title = "Task Instance Details"
        return self.render_template(
            'airflow/task.html',
            task_attrs=task_attrs,
            ti_attrs=ti_attrs,
            failed_dep_reasons=failed_dep_reasons or no_failed_deps_result,
            task_id=task_id,
            execution_date=execution_date,
            special_attrs_rendered=special_attrs_rendered,
            form=form,
            root=root,
            dag=dag, title=title)

    @expose('/xcom')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def xcom(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        # Carrying execution_date through, even though it's irrelevant for
        # this context
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')
        dm_db = models.DagModel
        ti_db = models.TaskInstance
        dag = session.query(dm_db).filter(dm_db.dag_id == dag_id).first()
        ti = session.query(ti_db).filter(ti_db.dag_id == dag_id and ti_db.task_id == task_id).first()

        if not ti:
            flash(
                "Task [{}.{}] doesn't seem to exist"
                " at the moment".format(dag_id, task_id),
                "error")
            return redirect(url_for('Airflow.index'))

        xcomlist = session.query(XCom).filter(
            XCom.dag_id == dag_id, XCom.task_id == task_id,
            XCom.execution_date == dttm).all()

        attributes = []
        for xcom in xcomlist:
            if not xcom.key.startswith('_'):
                attributes.append((xcom.key, xcom.value))

        title = "XCom"
        return self.render_template(
            'airflow/xcom.html',
            attributes=attributes,
            task_id=task_id,
            execution_date=execution_date,
            form=form,
            root=root,
            dag=dag, title=title)

    @expose('/run', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def run(self):
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)

        execution_date = request.form.get('execution_date')
        execution_date = pendulum.parse(execution_date)
        ignore_all_deps = request.form.get('ignore_all_deps') == "true"
        ignore_task_deps = request.form.get('ignore_task_deps') == "true"
        ignore_ti_state = request.form.get('ignore_ti_state') == "true"

        from airflow.executors import get_default_executor
        executor = get_default_executor()
        valid_celery_config = False
        valid_kubernetes_config = False

        try:
            from airflow.executors.celery_executor import CeleryExecutor
            valid_celery_config = isinstance(executor, CeleryExecutor)
        except ImportError:
            pass

        try:
            from airflow.contrib.executors.kubernetes_executor import KubernetesExecutor
            valid_kubernetes_config = isinstance(executor, KubernetesExecutor)
        except ImportError:
            pass

        if not valid_celery_config and not valid_kubernetes_config:
            flash("Only works with the Celery or Kubernetes executors, sorry", "error")
            return redirect(origin)

        ti = models.TaskInstance(task=task, execution_date=execution_date)
        ti.refresh_from_db()

        # Make sure the task instance can be queued
        dep_context = DepContext(
            deps=QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        failed_deps = list(ti.get_failed_dep_statuses(dep_context=dep_context))
        if failed_deps:
            failed_deps_str = ", ".join(
                ["{}: {}".format(dep.dep_name, dep.reason) for dep in failed_deps])
            flash("Could not queue task instance for execution, dependencies not met: "
                  "{}".format(failed_deps_str),
                  "error")
            return redirect(origin)

        executor.start()
        executor.queue_task_instance(
            ti,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        executor.heartbeat()
        flash(
            "Sent {} to the message queue, "
            "it should start any moment now.".format(ti))
        return redirect(origin)

    @expose('/delete', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def delete(self):
        from airflow.api.common.experimental import delete_dag
        from airflow.exceptions import DagNotFound, DagFileExists

        dag_id = request.values.get('dag_id')
        origin = request.values.get('origin') or url_for('Airflow.index')

        try:
            delete_dag.delete_dag(dag_id)
        except DagNotFound:
            flash("DAG with id {} not found. Cannot delete".format(dag_id), 'error')
            return redirect(request.referrer)
        except DagFileExists:
            flash("Dag id {} is still in DagBag. "
                  "Remove the DAG file first.".format(dag_id),
                  'error')
            return redirect(request.referrer)

        flash("Deleting DAG with id {}. May take a couple minutes to fully"
              " disappear.".format(dag_id))

        # Upon success return to origin.
        return redirect(origin)

    @expose('/trigger', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def trigger(self, session=None):
        dag_id = request.values.get('dag_id')
        origin = request.values.get('origin') or url_for('Airflow.index')
        dag = session.query(models.DagModel).filter(models.DagModel.dag_id == dag_id).first()
        if not dag:
            flash("Cannot find dag {}".format(dag_id))
            return redirect(origin)

        execution_date = timezone.utcnow()
        run_id = "manual__{0}".format(execution_date.isoformat())

        dr = DagRun.find(dag_id=dag_id, run_id=run_id)
        if dr:
            flash("This run_id {} already exists".format(run_id))
            return redirect(origin)

        run_conf = {}

        dag.create_dagrun(
            run_id=run_id,
            execution_date=execution_date,
            state=State.RUNNING,
            conf=run_conf,
            external_trigger=True
        )

        flash(
            "Triggered {}, "
            "it should start any moment now.".format(dag_id))
        return redirect(origin)

    def _clear_dag_tis(self, dag, start_date, end_date, origin,
                       recursive=False, confirmed=False):
        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
            )

            flash("{0} task instances have been cleared".format(count))
            return redirect(origin)

        tis = dag.clear(
            start_date=start_date,
            end_date=end_date,
            include_subdags=recursive,
            include_parentdag=recursive,
            dry_run=True,
        )
        if not tis:
            flash("No task instances to clear", 'error')
            response = redirect(origin)
        else:
            details = "\n".join([str(t) for t in tis])

            response = self.render_template(
                'airflow/confirm.html',
                message=("Here's the list of task instances you are about "
                         "to clear:"),
                details=details)

        return response

    @expose('/clear', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def clear(self):
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        dag = dagbag.get_dag(dag_id)

        execution_date = request.form.get('execution_date')
        execution_date = pendulum.parse(execution_date)
        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('upstream') == "true"
        downstream = request.form.get('downstream') == "true"
        future = request.form.get('future') == "true"
        past = request.form.get('past') == "true"
        recursive = request.form.get('recursive') == "true"

        dag = dag.sub_dag(
            task_regex=r"^{0}$".format(task_id),
            include_downstream=downstream,
            include_upstream=upstream)

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=recursive, confirmed=confirmed)

    @expose('/dagrun_clear', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_clear(self):
        dag_id = request.form.get('dag_id')
        origin = request.form.get('origin')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == "true"

        dag = dagbag.get_dag(dag_id)
        execution_date = pendulum.parse(execution_date)
        start_date = execution_date
        end_date = execution_date

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=True, confirmed=confirmed)

    @expose('/blocked')
    @has_access
    @provide_session
    def blocked(self, session=None):
        DR = models.DagRun
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        payload = []
        if filter_dag_ids:
            dags = (
                session.query(DR.dag_id, sqla.func.count(DR.id))
                .filter(DR.state == State.RUNNING)
                .group_by(DR.dag_id)

            )
            if 'all_dags' not in filter_dag_ids:
                dags = dags.filter(DR.dag_id.in_(filter_dag_ids))
            dags = dags.all()

            for dag_id, active_dag_runs in dags:
                max_active_runs = 0
                if dag_id in dagbag.dags:
                    max_active_runs = dagbag.dags[dag_id].max_active_runs
                payload.append({
                    'dag_id': dag_id,
                    'active_dag_run': active_dag_runs,
                    'max_active_runs': max_active_runs,
                })
        return wwwutils.json_response(payload)

    def _mark_dagrun_state_as_failed(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = pendulum.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

        if not dag:
            flash('Cannot find DAG: {}'.format(dag_id), 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_failed(dag, execution_date, commit=confirmed)

        if confirmed:
            flash('Marked failed on {} task instances'.format(len(new_dag_state)))
            return redirect(origin)

        else:
            details = '\n'.join([str(t) for t in new_dag_state])

            response = self.render_template(
                'airflow/confirm.html',
                message=("Here's the list of task instances you are about to mark as failed"),
                details=details)

            return response

    def _mark_dagrun_state_as_success(self, dag_id, execution_date, confirmed, origin):
        if not execution_date:
            flash('Invalid execution date', 'error')
            return redirect(origin)

        execution_date = pendulum.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

        if not dag:
            flash('Cannot find DAG: {}'.format(dag_id), 'error')
            return redirect(origin)

        new_dag_state = set_dag_run_state_to_success(dag, execution_date,
                                                     commit=confirmed)

        if confirmed:
            flash('Marked success on {} task instances'.format(len(new_dag_state)))
            return redirect(origin)

        else:
            details = '\n'.join([str(t) for t in new_dag_state])

            response = self.render_template(
                'airflow/confirm.html',
                message=("Here's the list of task instances you are about to mark as success"),
                details=details)

            return response

    @expose('/dagrun_failed', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_failed(self):
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = request.form.get('origin')
        return self._mark_dagrun_state_as_failed(dag_id, execution_date,
                                                 confirmed, origin)

    @expose('/dagrun_success', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def dagrun_success(self):
        dag_id = request.form.get('dag_id')
        execution_date = request.form.get('execution_date')
        confirmed = request.form.get('confirmed') == 'true'
        origin = request.form.get('origin')
        return self._mark_dagrun_state_as_success(dag_id, execution_date,
                                                  confirmed, origin)

    def _mark_task_instance_state(self, dag_id, task_id, origin, execution_date,
                                  confirmed, upstream, downstream,
                                  future, past, state):
        dag = dagbag.get_dag(dag_id)
        task = dag.get_task(task_id)
        task.dag = dag

        execution_date = pendulum.parse(execution_date)

        if not dag:
            flash("Cannot find DAG: {}".format(dag_id))
            return redirect(origin)

        if not task:
            flash("Cannot find task {} in DAG {}".format(task_id, dag.dag_id))
            return redirect(origin)

        from airflow.api.common.experimental.mark_tasks import set_state

        if confirmed:
            altered = set_state(task=task, execution_date=execution_date,
                                upstream=upstream, downstream=downstream,
                                future=future, past=past, state=state,
                                commit=True)

            flash("Marked {} on {} task instances".format(state, len(altered)))
            return redirect(origin)

        to_be_altered = set_state(task=task, execution_date=execution_date,
                                  upstream=upstream, downstream=downstream,
                                  future=future, past=past, state=state,
                                  commit=False)

        details = "\n".join([str(t) for t in to_be_altered])

        response = self.render_template(
            "airflow/confirm.html",
            message=("Here's the list of task instances you are about to mark as {}:".format(state)),
            details=details)

        return response

    @expose('/failed', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def failed(self):
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        execution_date = request.form.get('execution_date')

        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('upstream') == "true"
        downstream = request.form.get('downstream') == "true"
        future = request.form.get('future') == "true"
        past = request.form.get('past') == "true"

        return self._mark_task_instance_state(dag_id, task_id, origin, execution_date,
                                              confirmed, upstream, downstream,
                                              future, past, State.FAILED)

    @expose('/success', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    def success(self):
        dag_id = request.form.get('dag_id')
        task_id = request.form.get('task_id')
        origin = request.form.get('origin')
        execution_date = request.form.get('execution_date')

        confirmed = request.form.get('confirmed') == "true"
        upstream = request.form.get('upstream') == "true"
        downstream = request.form.get('downstream') == "true"
        future = request.form.get('future') == "true"
        past = request.form.get('past') == "true"

        return self._mark_task_instance_state(dag_id, task_id, origin, execution_date,
                                              confirmed, upstream, downstream,
                                              future, past, State.SUCCESS)

    @expose('/tree')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def tree(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_downstream=False,
                include_upstream=True)

        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = timezone.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

        DR = models.DagRun
        dag_runs = (
            session.query(DR)
            .filter(
                DR.dag_id == dag.dag_id,
                DR.execution_date <= base_date)
            .order_by(DR.execution_date.desc())
            .limit(num_runs)
            .all()
        )
        dag_runs = {
            dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs}

        dates = sorted(list(dag_runs.keys()))
        max_date = max(dates) if dates else None
        min_date = min(dates) if dates else None

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        task_instances = {}
        for ti in tis:
            tid = alchemy_to_dict(ti)
            dr = dag_runs.get(ti.execution_date)
            tid['external_trigger'] = dr['external_trigger'] if dr else False
            task_instances[(ti.task_id, ti.execution_date)] = tid

        expanded = []
        # The default recursion traces every path so that tree view has full
        # expand/collapse functionality. After 5,000 nodes we stop and fall
        # back on a quick DFS search for performance. See PR #320.
        node_count = [0]
        node_limit = 5000 / max(1, len(dag.roots))

        def recurse_nodes(task, visited):
            visited.add(task)
            node_count[0] += 1

            children = [
                recurse_nodes(t, visited) for t in task.upstream_list
                if node_count[0] < node_limit or t not in visited]

            # D3 tree uses children vs _children to define what is
            # expanded or not. The following block makes it such that
            # repeated nodes are collapsed by default.
            children_key = 'children'
            if task.task_id not in expanded:
                expanded.append(task.task_id)
            elif children:
                children_key = "_children"

            def set_duration(tid):
                if (isinstance(tid, dict) and tid.get("state") == State.RUNNING and
                        tid["start_date"] is not None):
                    d = timezone.utcnow() - pendulum.parse(tid["start_date"])
                    tid["duration"] = d.total_seconds()
                return tid

            return {
                'name': task.task_id,
                'instances': [
                    set_duration(task_instances.get((task.task_id, d))) or {
                        'execution_date': d.isoformat(),
                        'task_id': task.task_id
                    }
                    for d in dates],
                children_key: children,
                'num_dep': len(task.upstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'start_date': task.start_date,
                'end_date': task.end_date,
                'depends_on_past': task.depends_on_past,
                'ui_color': task.ui_color,
                'run_dag_id': task.run_dag_id,
            }

        data = {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [
                dag_runs.get(d) or {'execution_date': d.isoformat()}
                for d in dates],
        }

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        return self.render_template(
            'airflow/tree.html',
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            root=root,
            form=form,
            dag=dag, data=data, blur=blur, num_runs=num_runs)

    @expose('/graph')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def graph(self, session=None):
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        arrange = request.args.get('arrange', dag.orientation)

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {
                    'label': task.task_id,
                    'labelStyle': "fill:{0};".format(task.ui_fgcolor),
                    'style': "fill:{0};".format(task.ui_color),
                    'rx': 5,
                    'ry': 5,
                }
            })

        def get_upstream(task):
            for t in task.upstream_list:
                edge = {
                    'u': t.task_id,
                    'v': task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_upstream(t)

        for t in dag.roots:
            get_upstream(t)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dt_nr_dr_data['arrange'] = arrange
        dttm = dt_nr_dr_data['dttm']

        class GraphForm(DateTimeWithNumRunsWithDagRunsForm):
            arrange = SelectField("Layout", choices=(
                ('LR', "Left->Right"),
                ('RL', "Right->Left"),
                ('TB', "Top->Bottom"),
                ('BT', "Bottom->Top"),
            ))

        form = GraphForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm, session=session)}
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
                'description': t.description,
                'run_dag_id': t.run_dag_id,
            }
            for t in dag.tasks}
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        doc_md = markdown.markdown(dag.doc_md) \
            if hasattr(dag, 'doc_md') and dag.doc_md else ''

        return self.render_template(
            'airflow/graph.html',
            dag=dag,
            form=form,
            width=request.args.get('width', "100%"),
            height=request.args.get('height', "800"),
            execution_date=dttm.isoformat(),
            state_token=wwwutils.state_token(dt_nr_dr_data['dr_state']),
            doc_md=doc_md,
            arrange=arrange,
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            blur=blur,
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges)

    @expose('/graph-popover')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def graph_popover(self, session=None):
        '''
            Almost a copy of graph method.

        This view is used to show the graph preview in a popover for DAGOperator.
        '''
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if dag_id not in dagbag.dags:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        arrange = request.args.get('arrange', dag.orientation)

        nodes = []
        edges = []
        for task in dag.tasks:
            nodes.append({
                'id': task.task_id,
                'value': {
                    'label': task.task_id,
                    'labelStyle': "fill:{0};".format(task.ui_fgcolor),
                    'style': "fill:{0};".format(task.ui_color),
                    'rx': 5,
                    'ry': 5,
                }
            })

        def get_upstream(task):
            for t in task.upstream_list:
                edge = {
                    'u': t.task_id,
                    'v': task.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_upstream(t)

        for t in dag.roots:
            get_upstream(t)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dt_nr_dr_data['arrange'] = arrange
        dttm = dt_nr_dr_data['dttm']

        class GraphForm(DateTimeWithNumRunsWithDagRunsForm):
            arrange = SelectField("Layout", choices=(
                ('LR', "Left->Right"),
                ('RL', "Right->Left"),
                ('TB', "Top->Bottom"),
                ('BT', "Bottom->Top"),
            ))

        form = GraphForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm, session=session)}
        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
                'description': t.description,
                'run_dag_id': t.run_dag_id,
            }
            for t in dag.tasks}
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        doc_md = markdown.markdown(dag.doc_md) \
            if hasattr(dag, 'doc_md') and dag.doc_md else ''

        return self.render_template(
            'airflow/graph_popover.html',
            dag_id=dag_id,
            dag=dag,
            form=form,
            width=request.args.get('width', "100%"),
            height=request.args.get('height', "300"),
            execution_date=dttm.isoformat(),
            state_token=wwwutils.state_token(dt_nr_dr_data['dr_state']),
            doc_md=doc_md,
            arrange=arrange,
            operators=sorted(
                list(set([op.__class__ for op in dag.tasks])),
                key=lambda x: x.__name__
            ),
            blur=blur,
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges)

    @expose('/duration')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def duration(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if dag is None:
            flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return redirect(url_for('Airflow.index'))

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        cum_chart = nvd3.lineChart(
            name="cumLineChart", x_is_date=True, height=chart_height, width="1200")

        y = defaultdict(list)
        x = defaultdict(list)
        cum_y = defaultdict(list)

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        TF = TaskFail
        ti_fails = (
            session.query(TF)
                .filter(TF.dag_id == dag.dag_id,  # noqa
                        TF.execution_date >= min_date,
                        TF.execution_date <= base_date,
                        TF.task_id.in_([t.task_id for t in dag.tasks]))
                .all()  # noqa
        )

        fails_totals = defaultdict(int)
        for tf in ti_fails:
            dict_key = (tf.dag_id, tf.task_id, tf.execution_date)
            fails_totals[dict_key] += tf.duration

        for ti in tis:
            if ti.duration:
                dttm = wwwutils.epoch(ti.execution_date)
                x[ti.task_id].append(dttm)
                y[ti.task_id].append(float(ti.duration))
                fails_dict_key = (ti.dag_id, ti.task_id, ti.execution_date)
                fails_total = fails_totals[fails_dict_key]
                cum_y[ti.task_id].append(float(ti.duration + fails_total))

        # determine the most relevant time unit for the set of task instance
        # durations for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        cum_y_unit = infer_time_unit([d for t in cum_y.values() for d in t])
        # update the y Axis on both charts to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Duration ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '40'
        cum_chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                                label='Duration ({})'.format(cum_y_unit))
        cum_chart.axislist['yAxis']['axisLabelDistance'] = '40'

        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))
                cum_chart.add_serie(name=task.task_id, x=x[task.task_id],
                                    y=scale_time_units(cum_y[task.task_id],
                                                       cum_y_unit))

        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        cum_chart.buildcontent()
        s_index = cum_chart.htmlcontent.rfind('});')
        cum_chart.htmlcontent = (cum_chart.htmlcontent[:s_index] +
                                 "$( document ).trigger('chartload')" +
                                 cum_chart.htmlcontent[s_index:])

        return self.render_template(
            'airflow/duration_chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent,
            cum_chart=cum_chart.htmlcontent
        )

    @expose('/tries')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def tries(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, y_axis_format='d', height=chart_height,
            width="1200")

        for task in dag.tasks:
            y = []
            x = []
            for ti in task.get_task_instances(start_date=min_date,
                                              end_date=base_date,
                                              session=session):
                dttm = wwwutils.epoch(ti.execution_date)
                x.append(dttm)
                y.append(ti.try_number)
            if x:
                chart.add_serie(name=task.task_id, x=x, y=y)

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        tries = sorted(list({ti.try_number for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if tries else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})

        chart.buildcontent()

        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
            chart=chart.htmlcontent
        )

    @expose('/landing_times')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def landing_times(self, session=None):
        default_dag_run = conf.getint('webserver', 'default_dag_run_display_number')
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        base_date = request.args.get('base_date')
        num_runs = request.args.get('num_runs')
        num_runs = int(num_runs) if num_runs else default_dag_run

        if base_date:
            base_date = pendulum.parse(base_date)
        else:
            base_date = dag.latest_execution_date or timezone.utcnow()

        dates = dag.date_range(base_date, num=-abs(num_runs))
        min_date = dates[0] if dates else timezone.utc_epoch()

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        chart_height = wwwutils.get_chart_height(dag)
        chart = nvd3.lineChart(
            name="lineChart", x_is_date=True, height=chart_height, width="1200")
        y = {}
        x = {}
        for task in dag.tasks:
            y[task.task_id] = []
            x[task.task_id] = []
            for ti in task.get_task_instances(start_date=min_date,
                                              end_date=base_date,
                                              session=session):
                ts = ti.execution_date
                if dag.schedule_interval and dag.following_schedule(ts):
                    ts = dag.following_schedule(ts)
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x[ti.task_id].append(dttm)
                    y[ti.task_id].append(secs)

        # determine the most relevant time unit for the set of landing times
        # for the DAG
        y_unit = infer_time_unit([d for t in y.values() for d in t])
        # update the y Axis to have the correct time units
        chart.create_y_axis('yAxis', format='.02f', custom_format=False,
                            label='Landing Time ({})'.format(y_unit))
        chart.axislist['yAxis']['axisLabelDistance'] = '40'
        for task in dag.tasks:
            if x[task.task_id]:
                chart.add_serie(name=task.task_id, x=x[task.task_id],
                                y=scale_time_units(y[task.task_id], y_unit))

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        dates = sorted(list({ti.execution_date for ti in tis}))
        max_date = max([ti.execution_date for ti in tis]) if dates else None

        session.commit()

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        chart.buildcontent()
        return self.render_template(
            'airflow/chart.html',
            dag=dag,
            chart=chart.htmlcontent,
            height=str(chart_height + 100) + "px",
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            root=root,
            form=form,
        )

    @expose('/paused', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def paused(self, session=None):
        DagModel = models.DagModel
        dag_id = request.args.get('dag_id')
        orm_dag = (
            session.query(DagModel)
            .filter(DagModel.dag_id == dag_id).first()
        )
        if request.args.get('is_paused') == 'false':
            orm_dag.is_paused = True
        else:
            orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

        dagbag.get_dag(dag_id)
        return "OK"

    @expose('/refresh', methods=['POST'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def refresh(self, session=None):
        DagModel = models.DagModel
        dag_id = request.values.get('dag_id')
        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == dag_id).first()

        if orm_dag:
            orm_dag.last_expired = timezone.utcnow()
            session.merge(orm_dag)
        session.commit()

        # sync dag permission
        appbuilder.sm.sync_perm_for_dag(dag_id)

        dagbag.get_dag(dag_id)
        flash("DAG [{}] is now fresh as a daisy".format(dag_id))
        return redirect(request.referrer)

    @expose('/refresh_all', methods=['POST'])
    @has_access
    @action_logging
    def refresh_all(self):
        dagbag.collect_dags(only_if_updated=False)
        # sync permissions for all dags
        appbuilder.sm.sync_perm_for_dag()
        flash("All DAGs are now up to date")
        return redirect(url_for('Airflow.index'))

    @expose('/gantt')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def gantt(self, session=None):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)
        demo_mode = conf.getboolean('webserver', 'demo_mode')

        root = request.args.get('root')
        if root:
            dag = dag.sub_dag(
                task_regex=root,
                include_upstream=True,
                include_downstream=False)

        dt_nr_dr_data = get_date_time_num_runs_dag_runs_form_data(request, session, dag)
        dttm = dt_nr_dr_data['dttm']

        form = DateTimeWithNumRunsWithDagRunsForm(data=dt_nr_dr_data)
        form.execution_date.choices = dt_nr_dr_data['dr_choices']

        tis = [
            ti for ti in dag.get_task_instances(dttm, dttm, session=session)
            if ti.start_date]
        tis = sorted(tis, key=lambda ti: ti.start_date)
        TF = TaskFail
        ti_fails = list(itertools.chain(*[(
            session
            .query(TF)
            .filter(TF.dag_id == ti.dag_id,
                    TF.task_id == ti.task_id,
                    TF.execution_date == ti.execution_date)
            .all()
        ) for ti in tis]))

        # determine bars to show in the gantt chart
        # all reschedules of one attempt are combinded into one bar
        gantt_bar_items = []
        for ti in tis:
            end_date = ti.end_date or timezone.utcnow()
            gantt_bar_items.append((ti.task_id, ti.start_date, end_date, ti.state))
        for tf in ti_fails:
            end_date = tf.end_date or timezone.utcnow()
            gantt_bar_items.append((tf.task_id, tf.start_date, end_date, State.FAILED))

        tasks = []
        for gantt_bar_item in gantt_bar_items:
            task_id = gantt_bar_item[0]
            start_date = gantt_bar_item[1]
            end_date = gantt_bar_item[2]
            state = gantt_bar_item[3]
            tasks.append({
                'startDate': wwwutils.epoch(start_date),
                'endDate': wwwutils.epoch(end_date),
                'isoStart': start_date.isoformat()[:-4],
                'isoEnd': end_date.isoformat()[:-4],
                'taskName': task_id,
                'duration': "{}".format(end_date - start_date)[:-4],
                'status': state,
                'executionDate': dttm.isoformat(),
            })
        states = {ti.state: ti.state for ti in tis}
        data = {
            'taskNames': [ti.task_id for ti in tis],
            'tasks': tasks,
            'taskStatus': states,
            'height': len(tis) * 25 + 25,
        }

        session.commit()

        return self.render_template(
            'airflow/gantt.html',
            dag=dag,
            execution_date=dttm.isoformat(),
            form=form,
            data=data,
            base_date='',
            demo_mode=demo_mode,
            root=root,
        )

    @expose('/_logout')
    @action_logging
    def logout(self, session=None):
        return redirect(appbuilder.get_url_for_logout)

    @expose('/_login')
    @action_logging
    def login(self, session=None):
        return redirect(appbuilder.get_url_for_login)

    @expose('/object/task_instances')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def task_instances(self, session=None):
        dag_id = request.args.get('dag_id')
        dag = dagbag.get_dag(dag_id)

        dttm = request.args.get('execution_date')
        if dttm:
            dttm = pendulum.parse(dttm)
        else:
            return "Error: Invalid execution_date"

        task_instances = {
            ti.task_id: alchemy_to_dict(ti)
            for ti in dag.get_task_instances(dttm, dttm, session=session)}

        return json.dumps(task_instances)


class VersionView(AirflowBaseView):
    @expose('/version')
    @has_access
    def version(self):
        try:
            airflow_version = airflow.__version__
        except Exception as e:
            airflow_version = None
            logging.error(e)

        # Get the Git repo and git hash
        git_version = None
        try:
            with open(os.path.join(*[settings.AIRFLOW_HOME,
                                     'airflow', 'git_version'])) as f:
                git_version = f.readline()
        except Exception as e:
            logging.error(e)

        # Render information
        title = "Version Info"
        return self.render_template(
            'airflow/version.html',
            title=title,
            airflow_version=airflow_version,
            git_version=git_version)


class ConfigurationView(AirflowBaseView):
    @expose('/configuration')
    @has_access
    def conf(self):
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = conf.AIRFLOW_CONFIG
        # Don't show config when expose_config variable is False in airflow config
        if conf.getboolean("webserver", "expose_config"):
            with open(conf.AIRFLOW_CONFIG, 'r') as f:
                config = f.read()
            table = [(section, key, value, source)
                     for section, parameters in conf.as_dict(True, True).items()
                     for key, (value, source) in parameters.items()]
        else:
            config = (
                "# Your Airflow administrator chose not to expose the "
                "configuration, most likely for security reasons.")
            table = None

        if raw:
            return Response(
                response=config,
                status=200,
                mimetype="application/text")
        else:
            code_html = Markup(highlight(
                config,
                lexers.IniLexer(),  # Lexer call
                HtmlFormatter(noclasses=True))
            )
            return self.render_template(
                'airflow/config.html',
                pre_subtitle=settings.HEADER + "  v" + airflow.__version__,
                code_html=code_html, title=title, subtitle=subtitle,
                table=table)


class SparkConfView(AirflowBaseView):
    @expose('/couture_config', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_spark_conf(self):
        import collections
        import configparser as CP
        from airflow.configuration import AIRFLOW_HOME
        config = CP.ConfigParser()
        config.optionxform = str
        conf_path = AIRFLOW_HOME + '/couture-spark.conf'
        setup_path = AIRFLOW_HOME + '/../jars'
        keytab_path = AIRFLOW_HOME + '/keytab'

        config.read(filenames=conf_path)
        # orderedDictionary used so that the order displayed is same as in file
        args = collections.OrderedDict(config.items('arguments'))
        configs = collections.OrderedDict(config.items('configurations'))  # dictionary created
        title = "Couture Spark Configuration"

        files = []
        py_files = []
        for r, d, f in os.walk(setup_path):
            for file in f:
                if file.endswith(".jar"):
                    files.append(file)
                if file.endswith(".py") or file.endswith(".egg") or file.endswith(".zip"):
                    py_files.append(file)
        kt_files = []
        for r, d, f in os.walk(keytab_path):
            for file in f:
                kt_files.append(file)

        if request.method == 'POST':
            config.read(filenames=conf_path)
            # orderedDictionary used so that the order displayed is same as in file
            args = collections.OrderedDict(config.items('arguments'))
            configs = collections.OrderedDict(config.items('configurations'))  # dictionary created

            files = []
            py_files = []
            for r, d, f in os.walk(setup_path):
                for file in f:
                    if file.endswith(".jar"):
                        files.append(file)
                    if file.endswith(".py") or file.endswith(".egg") or file.endswith(".zip"):
                        py_files.append(file)
            kt_files = []
            for r, d, f in os.walk(keytab_path):
                for file in f:
                    kt_files.append(file)
            for i in args:
                if i != 'jars' and i != 'py-files' and i != 'keytab':
                    config.set('arguments', i, request.form[i])
                elif i == 'jars':  # if the field is jars
                    list_file = []
                    filenames = request.form.getlist('check')
                    for f in filenames:
                        fn = os.path.join(setup_path, f)  # joining the filenames with their path
                        list_file.append(fn)
                    jarfiles = ",".join(list_file)  # joining all the filenames in a string

                    config.set('arguments', i, jarfiles)  # saving the new updated list of files
                elif i == 'keytab':  # if the field is keytab
                    kt_file = []
                    filenames = request.form.getlist('kt_check')
                    for f in filenames:
                        fn = os.path.join(keytab_path, f)  # joining the filenames with their path
                        kt_file.append(fn)
                    ktfiles = ",".join(kt_file)  # joining all the filenames in a string

                    config.set('arguments', i, ktfiles)  # saving the new updated list of files
                else:
                    py_list_file = []
                    py_filenames = request.form.getlist('py_check')
                    for f in py_filenames:
                        fn = os.path.join(setup_path, f)  # joining the filenames with their path
                        py_list_file.append(fn)
                    pythonfiles = ",".join(py_list_file)  # joining all the filenames in a string

                    config.set('arguments', i, pythonfiles)  # saving the new updated list of files

            for j in configs:
                # print("printing j", j, request.form[j])
                config.set('configurations', j, request.form[j])  # saving the new updated fields

            # filtering out new keys:
            for key in request.form:
                if key.startswith('new-arg-key') and request.form[key]:
                    # adding new fields in config['arguments']
                    key_no = key.split('-')[-1]
                    config.set('arguments', request.form[key], request.form['new-arg-value-' + key_no])
                elif key.startswith('new-config-key') and request.form[key]:
                    # adding new fields in config['configurations']
                    key_no = key.split('-')[-1]
                    config.set('configurations', request.form[key],
                               request.form['new-config-value-' + key_no])

            try:
                # if there is option in the file, then delete
                if config.has_option('arguments', request.form['option_title_args_delete']):
                    # deleting from the config file
                    config.remove_option('arguments', request.form['option_title_args_delete'])
            except Exception:
                print("Sorry ! No field found in delete in args")

            try:
                # if there is option in the file, then delete
                if config.has_option('configurations', request.form['option_title_config_delete']):
                    # deleting from the config file
                    config.remove_option('configurations', request.form['option_title_config_delete'])
            except Exception:
                print("Sorry ! No field found in delete in config")

            # writing all the changes to the file
            with open(conf_path, 'w') as configfile:
                config.write(configfile)

            new_args = collections.OrderedDict(config.items('arguments'))
            new_config = collections.OrderedDict(config.items('configurations'))
            len_jar = len(files)
            len_py = len(py_files)
            kt_len = len(kt_files)
            # kt_files = []
            # kt_len = 0
            return self.render_template(
                'airflow/couture_config.html',
                title=title,
                Arguments=new_args,
                Configurations=new_config,
                Files=files,
                Py_Files=py_files,
                len_jar=len_jar,
                len_py=len_py,
                kt_len=kt_len,
                kt_Files=kt_files
            )
        else:
            files = []
            py_files = []
            for r, d, f in os.walk(setup_path):
                for file in f:
                    if file.endswith(".jar"):
                        files.append(file)
                    if file.endswith(".py") or file.endswith(".egg") or file.endswith(".zip"):
                        py_files.append(file)
            kt_files = []
            for r, d, f in os.walk(keytab_path):
                for file in f:
                    kt_files.append(file)
            len_jar = len(files)
            len_py = len(py_files)
            kt_len = len(kt_files)
            # kt_files = []
            # kt_len = 0
            return self.render_template(
                'airflow/couture_config.html',
                title=title,
                len=len(args),
                Arguments=args,
                Configurations=configs,
                Files=files, Py_Files=py_files,
                len_jar=len_jar, len_py=len_py,
                kt_len=kt_len,
                kt_Files=kt_files)


class HadoopConfView(AirflowBaseView):
    @expose('/hadoop_conn_file_list', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def hadoop_conn_file_list(self):
        import configparser as CP
        from airflow.configuration import AIRFLOW_HOME
        config = CP.ConfigParser()
        config.optionxform = str
        UPLOAD_FOLDER = AIRFLOW_HOME + '/../setup/hadoop_conn'

        file_data = {}
        # calling get_details with the extension as .xml
        file_data = AirflowBaseView.get_details(UPLOAD_FOLDER, ".xml")

        if request.method == 'POST':
            try:    # for deleting the xml files from the folder
                del_filename = request.form['option_title_delete']
                file_data = {}
                for r, d, f in os.walk(UPLOAD_FOLDER):
                    for file_name in f:
                        if file_name.endswith(".xml"):
                            if file_name == del_filename:
                                # removing  the file from the folder
                                os.remove(os.path.join(UPLOAD_FOLDER, file_name))
                                AirflowBaseView.audit_logging("hadoop_file_deleted", file_name,
                                                              request.environ['REMOTE_ADDR'])
                                flash('File Deleted!!', "warning")
                            else:
                                filePath = os.path.join(UPLOAD_FOLDER, file_name)
                                if(os.path.exists(filePath)):
                                    fileStatsObj = os.stat(filePath)
                                    modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])
                                    size = os.stat(filePath).st_size
                                    size = AirflowBaseView.convert_size(size)
                                    temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size}
                                    file_data[file_name] = temp_dict
                return self.render_template('airflow/hadoop_conn_file_list.html', file_data=file_data)
            except Exception:
                print("Sorry ! No file in xml for delete")

            target = os.path.join(UPLOAD_FOLDER)  # destination for uploading

            list_files = request.files.getlist("file")
            if len(list_files) > 0:
                flag = False
                for upload in request.files.getlist("file"):
                    filename = upload.filename
                    if filename.endswith('.xml'):  # for allowing only xml files to be uploaded
                        flag = True
                        destination = "/".join([target, filename])
                        upload.save(destination)
                        AirflowBaseView.audit_logging(
                            "hadoop_file_added", filename, request.environ['REMOTE_ADDR'])
                    else:
                        flash('Only XML files are supported!!', "error")
                if flag:
                    flash('File uploaded!!', "success")

                file_data = AirflowBaseView.get_details(UPLOAD_FOLDER, ".xml")
                return self.render_template('airflow/hadoop_conn_file_list.html', file_data=file_data)

        else:
            file_data = AirflowBaseView.get_details(UPLOAD_FOLDER, ".xml")
            return self.render_template('airflow/hadoop_conn_file_list.html', file_data=file_data)

    @expose("/hadoop_conn_file/<string:filename>", methods=['GET', 'POST'])
    def hadoop_file_edit(self, filename):
        from lxml import etree as ET
        from airflow.configuration import AIRFLOW_HOME
        UPLOAD_FOLDER = AIRFLOW_HOME + '/../setup/hadoop_conn'
        if request.method == 'GET':
            xml_file = os.path.join(UPLOAD_FOLDER, filename)
            tree = ET.parse(xml_file)
            root = tree.getroot()
            rootname = root.tag

            values_get = {}
            for p in root.iter('property'):
                name = p.find('name').text
                value = p.find('value').text
                values_get[name] = value  # storing all the name and value pairs in values_get dictionary
            return self.render_template('airflow/hadoop_conn_file.html',
                                        Section=rootname,
                                        Configurations=values_get,
                                        Filename=filename)

        if request.method == 'POST':
            xml_file = os.path.join(UPLOAD_FOLDER, filename)
            tree = ET.parse(xml_file)
            root = tree.getroot()

            values = {}
            for p in root.iter('property'):
                name = p.find('name').text
                value = p.find('value').text
                values[name] = value  # storing all the name and value pairs in values_get dictionary

            for prop in root.iter('property'):
                name = prop.find('name').text
                value = prop.find('value').text
                new_value = request.form[name]  # extracting updated values from the form
                prop.find('value').text = str(new_value)  # for saving edit changes in file

            try:   # for adding new properties in config file
                opttitleconfig = request.form['option_title_config']
                optvalueconfig = request.form['option_value_config']
                # for not adding empty fields in file
                if len(opttitleconfig) != 0 and len(optvalueconfig) != 0:
                    prop = ET.Element("property")
                    root.append(prop)
                    nm = ET.SubElement(prop, "name")
                    nm.text = request.form['option_title_config']
                    val = ET.SubElement(prop, "value")
                    val.text = request.form['option_value_config']
            except Exception:
                print("Sorry ! No field found ")

            try:
                del_name = request.form['option_title_config_delete']  # for deleting a property from file
                for p in root.iter('property'):
                    n = p.find('name').text
                    if n == del_name:
                        root.remove(p)
                    else:
                        print("no such field exists in config now.")
            except Exception:
                print("Sorry ! No field found in delete in config")

            tree.write(xml_file)  # writing all the updated changes to the fields

            xml_newfile = os.path.join(UPLOAD_FOLDER, filename)
            newtree = ET.parse(xml_newfile)
            newroot = newtree.getroot()
            newroot_name = newroot.tag

            newvalues = {}

            for pr in root.iter('property'):
                name = pr.find('name').text
                value = pr.find('value').text
                newvalues[name] = value

            return self.render_template('airflow/hadoop_conn_file.html',
                                        Section=newroot_name,
                                        Configurations=newvalues,
                                        Filename=filename)

    @expose("/hadoop_file_download/<string:filename>", methods=['GET', 'POST'])
    def download(self, filename):     # for downloading the file passed in the filename
        from airflow.configuration import AIRFLOW_HOME
        UPLOAD_FOLDER = AIRFLOW_HOME + '/../setup/hadoop_conn'

        path_file = os.path.join(UPLOAD_FOLDER, filename)
        return send_file(path_file, as_attachment=True)


class LdapConfView(AirflowBaseView):
    @expose('/ldap', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_ldap_conf(self):
        import collections
        import configparser as CP
        from airflow.configuration import AIRFLOW_HOME
        config = CP.ConfigParser()
        config.optionxform = str
        conf_path = AIRFLOW_HOME + '/ldap.conf'

        title = "Ldap Configuration"

        if request.method == 'POST':
            config.read(filenames=conf_path)
            args = collections.OrderedDict(config.items('ldap'))
            for i in args:
                config.set('ldap', i, request.form[i])
            with open(conf_path, 'w') as configfile:
                config.write(configfile)
            new_args = collections.OrderedDict(config.items('ldap'))
            return self.render_template(
                'airflow/ldap.html', title=title, Arguments=new_args)
        else:
            try:
                config.read(filenames=conf_path)
                args = collections.OrderedDict(config.items('ldap'))
                return self.render_template(
                    'airflow/ldap.html', title=title, Arguments=args)
            except CP.NoSectionError:
                return self.render_template(
                    'airflow/ldap.html', title=title, error='No LDAP Config Found')
            except Exception:
                error = '''Error while parsing LDAP conf file. Please check the
                           file and try again.'''
                return self.render_template(
                    'airflow/ldap.html', title=title, error=error)


class SparkDepView(AirflowBaseView):
    @expose('/spark_dependencies', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_artifact(self):
        title = "Spark Dependencies"
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/../jars'

        if request.method == 'POST':
            try:    # for deleting the files from the folder
                del_filename = request.form['option_title_delete_Artifact']
                file_data = {}
                for r, d, f in os.walk(add_to_dir):
                    for file_name in f:
                        if file_name == del_filename:
                            os.remove(os.path.join(add_to_dir, file_name))
                            AirflowBaseView.audit_logging(
                                "spark_dependency_deleted", file_name, request.environ['REMOTE_ADDR'])
                            flash('File Deleted!!', "warning")
                        else:
                            filePath = os.path.join(add_to_dir, file_name)
                            if(os.path.exists(filePath)):
                                fileStatsObj = os.stat(filePath)
                                modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])
                                size = os.stat(filePath).st_size
                                size = AirflowBaseView.convert_size(size)
                                temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size}
                                file_data[file_name] = temp_dict

                len_py = AirflowBaseView.get_len_py(file_data)
                len_jar = AirflowBaseView.get_len_jar(file_data)

                return self.render_template('airflow/spark_dependencies.html',
                                            title=title,
                                            file_data=file_data,
                                            len_jar=len_jar,
                                            len_py=len_py)

            except Exception:
                print("Sorry ! No file in xml for delete")

            target = os.path.join(add_to_dir)  # destination to save files
            if not os.path.isdir(target):
                os.mkdir(target)

            try:
                for f in request.files.getlist("file"):   # for saving a file
                    filename = f.filename
                    if filename.endswith((".py", ".egg", ".zip", ".jar",)):
                        destination = "/".join([target, filename])
                        f.save(destination)
                        AirflowBaseView.audit_logging("spark_dependency_added",
                                                      filename, request.environ['REMOTE_ADDR'])
                        flash(
                            'File Uploaded!! To include this file for spark job, select it from spark configuration.', "success")  # noqa
                    else:
                        flash('Supported format for spark dependencies are .jar, .zip, .egg, or .py!!',
                              "error")
            except Exception:
                print("No file selected!")

            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, "")
            len_py = AirflowBaseView.get_len_py(file_data)
            len_jar = AirflowBaseView.get_len_jar(file_data)

            return self.render_template('airflow/spark_dependencies.html',
                                        title=title,
                                        file_data=file_data,
                                        len_jar=len_jar,
                                        len_py=len_py)

        else:
            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, "")
            len_py = AirflowBaseView.get_len_py(file_data)
            len_jar = AirflowBaseView.get_len_jar(file_data)

            return self.render_template('airflow/spark_dependencies.html',
                                        title=title,
                                        file_data=file_data,
                                        len_jar=len_jar,
                                        len_py=len_py)

    @expose("/spark_dep_download/<string:filename>", methods=['GET', 'POST'])
    def download(self, filename):  # for downloading the file passed in the filename
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/../jars'

        path_file = os.path.join(add_to_dir, filename)
        return send_file(path_file, as_attachment=True)


class HelpView(AirflowBaseView):
    @expose('/help')
    @has_access
    def help(self):
        try:
            return send_file('templates/airflow/Couture_AI_Workflow_Orchestrator.pdf',
                             'application/pdf',
                             as_attachment=False)
        except Exception as e:
            return str(e)

    # @expose('/load_help')
    # @has_access
    # def file_downloads(self):
    #     try:
    #         return self.render_template('airflow/help.html')
    #     except Exception as e:
    #         return str(e)


class KeyTabView(AirflowBaseView):
    @expose('/keytab', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_keytab(self):
        # title = "KeyTab"
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/keytab'
        file_name = AIRFLOW_HOME + '/keytab/keytab.conf'
        principal = ''
        keytab_files = ''

        import configparser as CP
        import collections
        config = CP.ConfigParser()
        config.optionxform = str
        config.read(filenames=file_name)
        arguments = collections.OrderedDict(config.items('arguments'))
        args = arguments

        if request.method == 'POST':
            config.read(filenames=file_name)
            arguments = collections.OrderedDict(config.items('arguments'))
            all_files = []
            for r, d, f in os.walk(add_to_dir):
                for file in f:
                    if file.endswith(".keytab"):
                        all_files.append(file)

            for i in arguments:
                if i == 'principal':
                    config.set('arguments', i, request.form[i])
                    principal = request.form[i]
                elif i != 'keytab':
                    config.set('arguments', i, request.form[i])
                elif i == 'keytab':
                    list_file = []
                    filenames = request.form.getlist('check')
                    for f in filenames:
                        fn = os.path.join(add_to_dir, f)
                        list_file.append(fn)
                    keytab_files = ",".join(list_file)
                    config.set('arguments', i, keytab_files)

            try:
                if config.has_option('arguments', request.form[
                        'option_title_args_delete']):  # if there is option in the file, then delete
                    # deleting from the config file
                    config.remove_option('arguments',
                                         request.form['option_title_args_delete'])
                else:
                    print("no such field exists in args now.")
            except Exception:
                print("Sorry ! No field found in delete in args")

            try:  # for deleting the keytab files from the folder
                del_filename = request.form['option_title_delete_Artifact']
                file_data = {}
                for r, d, f in os.walk(add_to_dir):
                    for file_name in f:
                        if file_name == del_filename:
                            os.remove(os.path.join(add_to_dir, file_name))
                            AirflowBaseView.audit_logging(
                                "keytab_deleted", file_name, request.environ['REMOTE_ADDR'])
                            flash('File Deleted!!', "warning")
                        else:
                            filePath = os.path.join(add_to_dir, file_name)
                            if os.path.exists(filePath) and filePath.endswith(".keytab"):
                                fileStatsObj = os.stat(filePath)
                                modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])
                                size = os.stat(filePath).st_size
                                size = AirflowBaseView.convert_size(size)
                                temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size}
                                file_data[file_name] = temp_dict
                len_keytab = len(file_data)

                return self.render_template('airflow/keytab.html',
                                            file_data=file_data,
                                            len_keytab=len_keytab,
                                            Files=all_files)
            except Exception:
                print("Sorry ! No file to delete")

            target = os.path.join(add_to_dir)
            if not os.path.isdir(target):
                os.mkdir(target)

            try:
                for f in request.files.getlist("file"):   # for saving a file
                    filename = f.filename
                    # if filename.endswith(".keytab"):
                    destination = "/".join([target, filename])
                    f.save(destination)
                    AirflowBaseView.audit_logging("keytab_added", filename, request.environ['REMOTE_ADDR'])
                    flash('File Uploaded!!',
                          "success")
            except Exception:
                print("No file selected!")

            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, ".keytab")
            len_keytab = len(file_data)

            file_name = AIRFLOW_HOME + '/keytab/keytab.conf'
            with open(file_name, 'w') as configfile:
                config.write(configfile)
            newargs = collections.OrderedDict(config.items('arguments'))

            conf_path = AIRFLOW_HOME + '/couture-spark.conf'
            config.read(filenames=conf_path)
            config.set('arguments', 'principal', principal)
            config.set('arguments', 'keytab', keytab_files)
            with open(conf_path, 'w') as configfile:
                config.write(configfile)

            return self.render_template('airflow/keytab.html',
                                        file_data=file_data,
                                        len_keytab=len_keytab,
                                        Arguments=newargs,
                                        Files=all_files)

        else:
            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, ".keytab")
            len_keytab = len(file_data)
            all_files = []
            for r, d, f in os.walk(add_to_dir):
                for file in f:
                    if file.endswith(".keytab"):
                        all_files.append(file)
            return self.render_template('airflow/keytab.html',
                                        file_data=file_data,
                                        Arguments=args,
                                        len_keytab=len_keytab,
                                        Files=all_files)

    @expose("/keytab_download/<string:filename>", methods=['GET', 'POST'])
    def download(self, filename):  # for downloading the file passed in the filename
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/keytab'
        path_file = os.path.join(add_to_dir, filename)
        return send_file(path_file, as_attachment=True)


class JupyterNotebookView(AirflowBaseView):
    @expose('/jupyter_notebook')
    @has_access
    def jupyter_notebook(self):
        title = "Jupyter Notebook"
        return self.render_template('airflow/jupyter_notebook.html', title=title)


class CodeArtifactView(AirflowBaseView):
    @expose('/code_artifact', methods=['GET', 'POST'])
    @action_logging
    def update_spark_file(self):
        title = "Code Artifact"
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/../code'

        if request.method == 'POST':

            try:    # for deleting the files from the folder
                del_filename = request.form['option_title_delete_Spark']
                file_data = {}
                for r, d, f in os.walk(add_to_dir):
                    for file_name in f:
                        if file_name == del_filename:
                            os.remove(os.path.join(add_to_dir, file_name))
                            AirflowBaseView.audit_logging(
                                "code_artifact_deleted", file_name, request.environ['REMOTE_ADDR'])
                            flash('File Deleted!!', "warning")
                        else:
                            filePath = os.path.join(add_to_dir, file_name)
                            if(os.path.exists(filePath)):
                                fileStatsObj = os.stat(filePath)
                                modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])
                                size = os.stat(filePath).st_size
                                size = AirflowBaseView.convert_size(size)
                                temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size}
                                file_data[file_name] = temp_dict
                len_jar = AirflowBaseView.get_len_jar(file_data)
                len_py = AirflowBaseView.get_len_py(file_data)
                return self.render_template('airflow/code_artifact.html',
                                            title=title,
                                            len_jar=len_jar,
                                            len_py=len_py,
                                            file_data=file_data)
            except Exception:
                print("Sorry ! No file in spark for delete")

            target = os.path.join(add_to_dir)
            if not os.path.isdir(target):
                os.mkdir(target)

            try:
                for f in request.files.getlist("file"):   # for saving a file
                    filename = f.filename
                    if filename.endswith((".py", ".egg", ".zip", ".jar")):
                        destination = "/".join([target, filename])
                        f.save(destination)
                        AirflowBaseView.audit_logging(
                            "code_artifact_added", filename, request.environ['REMOTE_ADDR'])
                        flash('File Uploaded!!', "success")
                    else:
                        flash('Supported format for code artifacts are .jar, .py or .r!!', "error")
            except Exception:
                print("No file selected!")

            file_data = {}
            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, "")
            len_jar = AirflowBaseView.get_len_jar(file_data)
            len_py = AirflowBaseView.get_len_py(file_data)

            return self.render_template('airflow/code_artifact.html',
                                        title=title,
                                        len_jar=len_jar,
                                        len_py=len_py,
                                        file_data=file_data)
        else:
            file_data = {}
            # calling get_details without any extension
            file_data = AirflowBaseView.get_details(add_to_dir, "")
            len_jar = AirflowBaseView.get_len_jar(file_data)
            len_py = AirflowBaseView.get_len_py(file_data)

            return self.render_template('airflow/code_artifact.html',
                                        title=title,
                                        len_jar=len_jar,
                                        len_py=len_py,
                                        file_data=file_data)

    @expose("/artifact_download/<string:filename>", methods=['GET', 'POST'])
    def download(self, filename):        # for downloading the file passed in the filename
        from airflow.configuration import AIRFLOW_HOME
        add_to_dir = AIRFLOW_HOME + '/../code'
        path_file = os.path.join(add_to_dir, filename)
        return send_file(path_file, as_attachment=True)


class AddDagView(AirflowBaseView):
    # WARNING: No access control decorators found in this view.
    # TODO: Add access control decorators.

    # regex for validating filenames while adding new ones
    regex_valid_filenames = re.compile('^[A-Za-z0-9_@()-]+$')

    template_dag_file_path = os.path.join(
        app.root_path, *['..', 'config_templates', 'default_dag_template.py'])
    dag_file_template = ''
    try:
        with open(template_dag_file_path, 'r') as f:
            dag_file_template = f.read()
    except Exception:
        pass

    def get_dag_file_path(self, filename):
        return os.path.join(settings.DAGS_FOLDER, filename)

    def get_snippet_file_path(self):
        return os.path.join(AIRFLOW_HOME, *['repo', 'dag-snippets.json'])

    def get_snippets(self):
        snippets_path = self.get_snippet_file_path()
        if Path(snippets_path).exists():
            with open(snippets_path) as f:
                return json.load(f)
        return dict()

    @expose('/add_dag', methods=['GET', 'POST'])
    @action_logging
    @has_access
    def add_dag(self):
        """This view adds or removes DAG files.
        """
        # TODO: Refactor this code further.
        # TODO: Change name of this view.
        title = "Add DAG"
        dags_dir = settings.DAGS_FOLDER

        if request.method == 'GET':
            del_filename = request.args.get('delete')
            if del_filename:
                # This will only scan the current directory. If we want to
                # scan sub directories, we use `os.walk` instead.
                for file_name in os.listdir(dags_dir):
                    if file_name.endswith(".py") and file_name == del_filename:
                        os.remove(self.get_dag_file_path(file_name))
                        AirflowBaseView.audit_logging('dag_deleted',
                                                      file_name,
                                                      request.environ['REMOTE_ADDR'])
                        flash('File ' + file_name + ' Deleted!!', "warning")
                        break
        elif request.method == 'POST':
            list_files = request.files.getlist("file")
            # check if a new filename has been sent to be created.
            filename = request.form.get('filename')
            if len(list_files) > 0:
                files_uploaded = 0
                for upload in request.files.getlist("file"):
                    filename = upload.filename
                    if self.regex_valid_filenames.match(os.path.splitext(filename)[0]):
                        destination = self.get_dag_file_path(filename)
                        upload.save(destination)
                        AirflowBaseView.audit_logging('dag_added',
                                                      filename,
                                                      request.environ['REMOTE_ADDR'])
                        files_uploaded += 1
                    elif filename:
                        flash('Only python files allowed !, ' + filename + ' not allowed', 'error')

                flash(str(files_uploaded) + ' files uploaded!!', 'success')

            elif filename:
                if self.regex_valid_filenames.match(filename):
                    filename = '.'.join([filename, 'py'])
                    try:
                        # don't overwrite existing files
                        Path(self.get_dag_file_path(filename)).touch(exist_ok=False)
                        if request.form.get('insert-template-content', None):
                            with open(self.get_dag_file_path(filename), 'w') as new_dag:
                                new_dag.write(self.dag_file_template)
                                AirflowBaseView.audit_logging('empty_dag_added',
                                                              filename,
                                                              request.environ['REMOTE_ADDR'])
                    except FileExistsError:
                        pass
                    return redirect(url_for('AddDagView.editdag', filename=filename))
                else:
                    flash('Invalid DAG name, DAG not created.', 'error')
            # the below redirect is to avoid form resubmission messages when
            # we refresh the page in the browser.
            return redirect(url_for('AddDagView.add_dag'))
        file_data = AirflowBaseView.get_details(dags_dir, ".py")
        return self.render_template('airflow/add_dag.html', title=title, file_data=file_data)

    @expose("/editdag/<string:filename>", methods=['GET', 'POST'])
    @action_logging
    @has_access
    def editdag(self, filename):
        fullpath = self.get_dag_file_path(filename)
        if not Path(fullpath).exists() and self.regex_valid_filenames.match(os.path.splitext(filename)[0]):
            return make_response(('DAG not found', 404))
        if request.method == 'POST':
            code = request.form['code']
            with open(fullpath, 'w') as code_file:
                code_file.write(code)
                flash('Successfully saved !')
            return redirect(url_for('AddDagView.editdag', filename=filename))
        else:
            with open(fullpath, 'r') as code_file:
                code = code_file.read()

        return self.render_template("airflow/editdag.html",
                                    code=code, filename=filename, snippets=self.get_snippets())

    @expose("/save_snippet/<string:filename>", methods=['POST'])
    @has_access
    def save_snippet(self, filename):
        snippet_file_path = self.get_snippet_file_path()

        # creating a path to tasks_folder (works for python >= 3.5)
        Path(snippet_file_path).parent.mkdir(parents=True, exist_ok=True)

        if request.method == 'POST':
            if Path(snippet_file_path).exists():
                with open(snippet_file_path) as f:
                    snippets = json.load(f)
            else:
                snippets = {}

            snippets[request.form['title']] = {
                'description': request.form['description'],
                'snippet': request.form['snippet']
            }

            with open(snippet_file_path, 'w') as f:
                json.dump(snippets, f)

            return redirect(url_for('AddDagView.editdag', filename=filename))
        return make_response(('METHOD_NOT_ALLOWED', 403))

    @expose("/dag_download/<string:filename>", methods=['GET', 'POST'])
    @has_access
    def download(self, filename):
        path_file = self.get_dag_file_path(filename)
        return send_file(path_file, as_attachment=True)


######################################################################################
#                                    ModelViews
######################################################################################

class DagFilter(BaseFilter):
    def apply(self, query, func):  # noqa
        if appbuilder.sm.has_all_dags_access():
            return query
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        return query.filter(self.model.dag_id.in_(filter_dag_ids))


class AirflowModelView(ModelView):
    list_widget = AirflowModelListWidget
    page_size = PAGE_SIZE

    CustomSQLAInterface = wwwutils.CustomSQLAInterface


class SlaMissModelView(AirflowModelView):
    route_base = '/slamiss'

    datamodel = AirflowModelView.CustomSQLAInterface(SlaMiss)

    base_permissions = ['can_list']

    list_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    add_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    edit_columns = ['dag_id', 'task_id', 'execution_date', 'email_sent', 'timestamp']
    search_columns = ['dag_id', 'task_id', 'email_sent', 'timestamp', 'execution_date']
    base_order = ('execution_date', 'desc')
    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'task_id': wwwutils.task_instance_link,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'timestamp': wwwutils.datetime_f('timestamp'),
        'dag_id': wwwutils.dag_link,
    }


class XComModelView(AirflowModelView):
    route_base = '/xcom'

    datamodel = AirflowModelView.CustomSQLAInterface(XCom)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    search_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    list_columns = ['key', 'value', 'timestamp', 'execution_date', 'task_id', 'dag_id']
    add_columns = ['key', 'value', 'execution_date', 'task_id', 'dag_id']
    edit_columns = ['key', 'value', 'execution_date', 'task_id', 'dag_id']
    base_order = ('execution_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    @action('muldelete', 'Delete', "Are you sure you want to delete selected records?",
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())


class ConnectionModelView(AirflowModelView):
    route_base = '/connection'

    datamodel = AirflowModelView.CustomSQLAInterface(Connection)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    extra_fields = ['extra__jdbc__drv_path', 'extra__jdbc__drv_clsname',
                    'extra__google_cloud_platform__project',
                    'extra__google_cloud_platform__key_path',
                    'extra__google_cloud_platform__keyfile_dict',
                    'extra__google_cloud_platform__scope',
                    'extra__google_cloud_platform__num_retries',
                    'extra__grpc__auth_type',
                    'extra__grpc__credential_pem_file',
                    'extra__grpc__scopes']
    list_columns = ['conn_id', 'conn_type', 'host', 'port', 'is_encrypted',
                    'is_extra_encrypted']
    add_columns = edit_columns = ['conn_id', 'conn_type', 'host', 'schema',
                                  'login', 'password', 'port', 'extra'] + extra_fields
    add_form = edit_form = ConnectionForm
    add_template = 'airflow/conn_create.html'
    edit_template = 'airflow/conn_edit.html'

    base_order = ('conn_id', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    @has_dag_access(can_dag_edit=True)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def process_form(self, form, is_created):
        formdata = form.data
        if formdata['conn_type'] in ['jdbc', 'google_cloud_platform', 'grpc']:
            extra = {
                key: formdata[key]
                for key in self.extra_fields if key in formdata}
            form.extra.data = json.dumps(extra)

    def prefill_form(self, form, pk):
        try:
            d = json.loads(form.data.get('extra', '{}'))
        except Exception:
            d = {}

        if not hasattr(d, 'get'):
            logging.warning('extra field for {} is not iterable'.format(
                form.data.get('conn_id', '<unknown>')))
            return

        for field in self.extra_fields:
            value = d.get(field, '')
            if value:
                field = getattr(form, field)
                field.data = value


class PoolModelView(AirflowModelView):
    route_base = '/pool'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Pool)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete']

    list_columns = ['pool', 'slots', 'used_slots', 'queued_slots']
    add_columns = ['pool', 'slots', 'description']
    edit_columns = ['pool', 'slots', 'description']

    base_order = ('pool', 'asc')

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pool_link(attr):
        pool_id = attr.get('pool')
        if pool_id is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id)
            return Markup("<a href='{url}'>{pool_id}</a>").format(url=url, pool_id=pool_id)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fused_slots(attr):
        pool_id = attr.get('pool')
        used_slots = attr.get('used_slots')
        if pool_id is not None and used_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='running')
            return Markup("<a href='{url}'>{used_slots}</a>").format(url=url, used_slots=used_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    def fqueued_slots(attr):
        pool_id = attr.get('pool')
        queued_slots = attr.get('queued_slots')
        if pool_id is not None and queued_slots is not None:
            url = url_for('TaskInstanceModelView.list', _flt_3_pool=pool_id, _flt_3_state='queued')
            return Markup("<a href='{url}'>{queued_slots}</a>").format(url=url, queued_slots=queued_slots)
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'pool': pool_link,
        'used_slots': fused_slots,
        'queued_slots': fqueued_slots
    }

    validators_columns = {
        'pool': [validators.DataRequired()],
        'slots': [validators.NumberRange(min=0)]
    }


class VariableModelView(AirflowModelView):
    route_base = '/variable'

    list_template = 'airflow/variable_list.html'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete', 'can_varimport']

    list_columns = ['key', 'val', 'is_encrypted']
    add_columns = ['key', 'val', 'is_encrypted']
    edit_columns = ['key', 'val']
    search_columns = ['key', 'val']

    base_order = ('key', 'asc')

    def hidden_field_formatter(attr):
        key = attr.get('key')
        val = attr.get('val')
        if wwwutils.should_hide_value_for_key(key):
            return Markup('*' * 8)
        if val:
            return val
        else:
            return Markup('<span class="label label-danger">Invalid</span>')

    formatters_columns = {
        'val': hidden_field_formatter,
    }

    validators_columns = {
        'key': [validators.DataRequired()]
    }

    def prefill_form(self, form, id):
        if wwwutils.should_hide_value_for_key(form.key.data):
            form.val.data = '*' * 8

    @action('muldelete', 'Delete', 'Are you sure you want to delete selected records?',
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('varexport', 'Export', '', single=False)
    def action_varexport(self, items):
        var_dict = {}
        d = json.JSONDecoder()
        for var in items:
            try:
                val = d.decode(var.val)
            except Exception:
                val = var.val
            var_dict[var.key] = val

        response = make_response(json.dumps(var_dict, sort_keys=True, indent=4))
        response.headers["Content-Disposition"] = "attachment; filename=variables.json"
        return response

    @expose('/varimport', methods=["POST"])
    @has_access
    @action_logging
    def varimport(self):
        try:
            out = request.files['file'].read()
            if not PY2 and isinstance(out, bytes):
                d = json.loads(out.decode('utf-8'))
            else:
                d = json.loads(out)
        except Exception:
            self.update_redirect()
            flash("Missing file or syntax error.", 'error')
            return redirect(self.get_redirect())
        else:
            suc_count = fail_count = 0
            for k, v in d.items():
                try:
                    models.Variable.set(k, v, serialize_json=isinstance(v, dict))
                except Exception as e:
                    logging.info('Variable import failed: {}'.format(repr(e)))
                    fail_count += 1
                else:
                    suc_count += 1
            flash("{} variable(s) successfully updated.".format(suc_count))
            if fail_count:
                flash("{} variable(s) failed to be updated.".format(fail_count), 'error')
            self.update_redirect()
            return redirect(self.get_redirect())


class JobModelView(AirflowModelView):
    route_base = '/job'

    datamodel = AirflowModelView.CustomSQLAInterface(jobs.BaseJob)

    base_permissions = ['can_list']

    list_columns = ['id', 'dag_id', 'state', 'job_type', 'start_date',
                    'end_date', 'latest_heartbeat',
                    'executor_class', 'hostname', 'unixname']
    search_columns = ['id', 'dag_id', 'state', 'job_type', 'start_date',
                      'end_date', 'latest_heartbeat', 'executor_class',
                      'hostname', 'unixname']

    base_order = ('start_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'latest_heartbeat': wwwutils.datetime_f('latest_heartbeat'),
    }


class DagRunModelView(AirflowModelView):
    route_base = '/dagrun'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagRun)

    base_permissions = ['can_list']

    list_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']
    search_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger']

    base_order = ('execution_date', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    add_form = edit_form = DagRunForm

    formatters_columns = {
        'execution_date': wwwutils.datetime_f('execution_date'),
        'state': wwwutils.state_f,
        'start_date': wwwutils.datetime_f('start_date'),
        'dag_id': wwwutils.dag_link,
        'run_id': wwwutils.dag_run_link,
    }

    validators_columns = {
        'dag_id': [validators.DataRequired()]
    }

    @action('muldelete', "Delete", "Are you sure you want to delete selected records?",
            single=False)
    @has_dag_access(can_dag_edit=True)
    @provide_session
    def action_muldelete(self, items, session=None):
        self.datamodel.delete_all(items)
        self.update_redirect()
        dirty_ids = []
        for item in items:
            dirty_ids.append(item.dag_id)
        return redirect(self.get_redirect())

    @action('set_running', "Set state to 'running'", '', single=False)
    @provide_session
    def action_set_running(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            for dr in session.query(DR).filter(
                    DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                dr.start_date = timezone.utcnow()
                dr.state = State.RUNNING
            session.commit()
            flash("{count} dag runs were set to running".format(count=count))
        except Exception as ex:
            flash(str(ex), 'error')
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_failed', "Set state to 'failed'",
            "All running task instances would also be marked as failed, are you sure?",
            single=False)
    @provide_session
    def action_set_failed(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DR).filter(
                    DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_failed(dagbag.get_dag(dr.dag_id),
                                                dr.execution_date,
                                                commit=True,
                                                session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to failed".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())

    @action('set_success', "Set state to 'success'",
            "All task instances would also be marked as success, are you sure?",
            single=False)
    @provide_session
    def action_set_success(self, drs, session=None):
        try:
            DR = models.DagRun
            count = 0
            dirty_ids = []
            altered_tis = []
            for dr in session.query(DR).filter(
                    DR.id.in_([dagrun.id for dagrun in drs])).all():
                dirty_ids.append(dr.dag_id)
                count += 1
                altered_tis += \
                    set_dag_run_state_to_success(dagbag.get_dag(dr.dag_id),
                                                 dr.execution_date,
                                                 commit=True,
                                                 session=session)
            altered_ti_count = len(altered_tis)
            flash(
                "{count} dag runs and {altered_ti_count} task instances "
                "were set to success".format(count=count, altered_ti_count=altered_ti_count))
        except Exception:
            flash('Failed to set state', 'error')
        return redirect(self.get_default_url())


class LogModelView(AirflowModelView):
    route_base = '/log'

    datamodel = AirflowModelView.CustomSQLAInterface(Log)

    base_permissions = ['can_list']

    list_columns = ['id', 'dttm', 'dag_id', 'task_id', 'event', 'execution_date',
                    'owner', 'extra', 'source_ip']
    search_columns = ['dag_id', 'task_id', 'execution_date', 'extra']

    base_order = ('dttm', 'desc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    formatters_columns = {
        'dttm': wwwutils.datetime_f('dttm'),
        'execution_date': wwwutils.datetime_f('execution_date'),
        'dag_id': wwwutils.dag_link,
    }


class TaskInstanceModelView(AirflowModelView):
    route_base = '/taskinstance'

    datamodel = AirflowModelView.CustomSQLAInterface(models.TaskInstance)

    base_permissions = ['can_list']

    page_size = PAGE_SIZE

    list_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'operator',
                    'start_date', 'end_date', 'duration', 'job_id', 'hostname',
                    'unixname', 'priority_weight', 'queue', 'queued_dttm', 'try_number',
                    'pool', 'log_url']

    search_columns = ['state', 'dag_id', 'task_id', 'execution_date', 'hostname',
                      'queue', 'pool', 'operator', 'start_date', 'end_date']

    base_order = ('job_id', 'asc')

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def log_url_formatter(attr):
        log_url = attr.get('log_url')
        return Markup(
            '<a href="{log_url}">'
            '    <span class="glyphicon glyphicon-book" aria-hidden="true">'
            '</span></a>').format(log_url=log_url)

    def duration_f(attr):
        end_date = attr.get('end_date')
        duration = attr.get('duration')
        if end_date and duration:
            return timedelta(seconds=duration)

    formatters_columns = {
        'log_url': log_url_formatter,
        'task_id': wwwutils.task_instance_link,
        'hostname': wwwutils.nobr_f('hostname'),
        'state': wwwutils.state_f,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'start_date': wwwutils.datetime_f('start_date'),
        'end_date': wwwutils.datetime_f('end_date'),
        'queued_dttm': wwwutils.datetime_f('queued_dttm'),
        'dag_id': wwwutils.dag_link,
        'duration': duration_f,
    }

    @provide_session
    @action('clear', lazy_gettext('Clear'),
            lazy_gettext('Are you sure you want to clear the state of the selected task'
                         ' instance(s) and set their dagruns to the running state?'),
            single=False)
    def action_clear(self, tis, session=None):
        try:
            dag_to_tis = {}

            for ti in tis:
                dag = dagbag.get_dag(ti.dag_id)
                tis = dag_to_tis.setdefault(dag, [])
                tis.append(ti)

            for dag, tis in dag_to_tis.items():
                models.clear_task_instances(tis, session=session, dag=dag)

            session.commit()
            flash("{0} task instances have been cleared".format(len(tis)))
            self.update_redirect()
            return redirect(self.get_redirect())

        except Exception:
            flash('Failed to clear task instances', 'error')

    @provide_session
    def set_task_instance_state(self, tis, target_state, session=None):
        try:
            count = len(tis)
            for ti in tis:
                ti.set_state(target_state, session=session)
            session.commit()
            flash("{count} task instances were set to '{target_state}'".format(
                count=count, target_state=target_state))
        except Exception:
            flash('Failed to set state', 'error')

    @action('set_running', "Set state to 'running'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_running(self, tis):
        self.set_task_instance_state(tis, State.RUNNING)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_failed', "Set state to 'failed'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_failed(self, tis):
        self.set_task_instance_state(tis, State.FAILED)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_success', "Set state to 'success'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_success(self, tis):
        self.set_task_instance_state(tis, State.SUCCESS)
        self.update_redirect()
        return redirect(self.get_redirect())

    @action('set_retry', "Set state to 'up_for_retry'", '', single=False)
    @has_dag_access(can_dag_edit=True)
    def action_set_retry(self, tis):
        self.set_task_instance_state(tis, State.UP_FOR_RETRY)
        self.update_redirect()
        return redirect(self.get_redirect())

    def get_one(self, id):
        """
        As a workaround for AIRFLOW-252, this method overrides Flask-Admin's
        ModelView.get_one().

        TODO: this method should be removed once the below bug is fixed on
        Flask-Admin side. https://github.com/flask-admin/flask-admin/issues/1226
        """
        task_id, dag_id, execution_date = iterdecode(id)  # noqa
        execution_date = pendulum.parse(execution_date)
        return self.session.query(self.model).get((task_id, dag_id, execution_date))


class DagModelView(AirflowModelView):
    route_base = '/dagmodel'

    datamodel = AirflowModelView.CustomSQLAInterface(models.DagModel)

    base_permissions = ['can_list', 'can_show', 'can_edit']

    list_columns = ['dag_id', 'is_paused', 'last_scheduler_run',
                    'last_expired', 'scheduler_lock', 'fileloc', 'owners']

    formatters_columns = {
        'dag_id': wwwutils.dag_link
    }

    base_filters = [['dag_id', DagFilter, lambda: []]]

    def get_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self).get_query()
            .filter(or_(models.DagModel.is_active,
                        models.DagModel.is_paused))
            .filter(~models.DagModel.is_subdag)
        )

    def get_count_query(self):
        """
        Default filters for model
        """
        return (
            super(DagModelView, self).get_count_query()
            .filter(models.DagModel.is_active)
            .filter(~models.DagModel.is_subdag)
        )
