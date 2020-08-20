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
import ast
import copy
import collections
import configparser as CP
import functools
import itertools
import json
import logging
import math
import os
import re
import requests
import socket
import shutil
import stat
import tarfile
import tempfile
import time
import threading
import traceback
import yaml
from collections import defaultdict
from datetime import timedelta, datetime
from urllib.parse import unquote
from pathlib import Path

import six
from six.moves.urllib.parse import quote

import markdown
import pendulum
import sqlalchemy as sqla
from croniter import CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError, croniter
from flask import (
    Markup, Response, escape, flash, jsonify, make_response, redirect, render_template, request,
    session as flask_session, url_for, g, send_file
)
from flask._compat import PY2
from flask_appbuilder import BaseView, ModelView, expose, has_access, permission_name
from flask_appbuilder.actions import action
from flask_appbuilder.api import BaseApi
# from flask_appbuilder.security.decorators import protect
from flask_appbuilder.models.sqla.filters import BaseFilter
from flask_babel import lazy_gettext
import lazy_object_proxy
from jinja2.utils import htmlsafe_json_dumps  # type: ignore
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import and_, desc, func, or_, union_all # noqa
from sqlalchemy.orm import joinedload
from wtforms import SelectField, validators

import airflow
from airflow.configuration import conf
from airflow import models, jobs
from airflow import settings, configuration
from airflow.api.common.experimental.mark_tasks import (set_dag_run_state_to_success,
                                                        set_dag_run_state_to_failed)
from airflow.jobs.backfill_job import BackfillJob
from airflow.models import Connection, DagModel, DagRun, DagTag, Log, SlaMiss, TaskFail, XCom, errors
from airflow.exceptions import AirflowException
from airflow.models.dagcode import DagCode
from airflow.ti_deps.dep_context import RUNNING_DEPS, SCHEDULER_QUEUED_DEPS, DepContext
from airflow.utils import timezone
from airflow.settings import STORE_SERIALIZED_DAGS
from airflow.configuration import AIRFLOW_HOME
from airflow.utils.dates import infer_time_unit, scale_time_units
from airflow.utils.db import provide_session, create_session
from airflow.utils.helpers import alchemy_to_dict, render_log_filename
from airflow.utils.state import State
from airflow._vendor import nvd3
from airflow.www_rbac import utils as wwwutils
from airflow.www_rbac.app import app, appbuilder, csrf
from airflow.www_rbac.decorators import action_logging, gzipped, has_dag_access
from airflow.www_rbac.forms import (DateTimeForm, DateTimeWithNumRunsForm,
                                    DateTimeWithNumRunsWithDagRunsForm,
                                    DagRunForm, ConnectionForm)
from airflow.www_rbac.mixins import GitIntegrationMixin
from airflow.www_rbac.widgets import AirflowModelListWidget
from airflow.www_rbac.utils import unpause_dag, move_to_hdfs


PAGE_SIZE = conf.getint('webserver', 'page_size')
FILTER_TAGS_COOKIE = 'tags_filter'
dagbag = None


def _parse_dags(update_DagModel=False):
    global dagbag
    if os.environ.get('SKIP_DAGS_PARSING') != 'True':
        dagbag = models.DagBag(settings.DAGS_FOLDER, store_serialized_dags=STORE_SERIALIZED_DAGS)
    else:
        dagbag = models.DagBag(os.devnull, include_examples=False)
    if update_DagModel:
        # Check if this works b/w multiple gunicorn wokers or not.
        for dag in dagbag.dags.values():
            dag.sync_to_db()


_parse_dags()

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
        'airflow/circles.html', hostname=socket.getfqdn() if conf.getboolean(
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact'), 404


@app.errorhandler(500)
def show_traceback(error):
    from airflow.utils import asciiart as ascii_
    return render_template(
        'airflow/traceback.html',
        hostname=socket.getfqdn() if conf.getboolean(
            'webserver',
            'EXPOSE_HOSTNAME',
            fallback=True) else 'redact',
        nukular=ascii_.nukular,
        info=traceback.format_exc() if conf.getboolean(
            'webserver',
            'EXPOSE_STACKTRACE',
            fallback=True) else 'Error! Please contact server admin'), 500


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

    def get_details(self, dir_path, file_extension):
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
                        size = AirflowBaseView.convert_size(size_bytes)
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

    # Make our macros available to our UI templates too.
    extra_args = {
        'macros': airflow.macros,
    }

    def render_template(self, *args, **kwargs):
        return super(AirflowBaseView, self).render_template(
            *args,
            # Cache this at most once per request, not for the lifetime of the view instanc
            scheduler_job=lazy_object_proxy.Proxy(jobs.SchedulerJob.most_recent_job),
            **kwargs
        )


class Airflow(AirflowBaseView):
    @expose('/health')
    def health(self):
        """
        An endpoint helping check the health status of the Airflow instance,
        including metadatabase and scheduler.
        """

        payload = {
            'metadatabase': {'status': 'unhealthy'}
        }
        latest_scheduler_heartbeat = None
        scheduler_status = 'unhealthy'
        payload['metadatabase'] = {'status': 'healthy'}
        try:
            scheduler_job = jobs.SchedulerJob.most_recent_job()

            if scheduler_job:
                latest_scheduler_heartbeat = scheduler_job.latest_heartbeat.isoformat()
                if scheduler_job.is_alive():
                    scheduler_status = 'healthy'
        except Exception:
            payload['metadatabase']['status'] = 'unhealthy'

        payload['scheduler'] = {'status': scheduler_status,
                                'latest_scheduler_heartbeat': latest_scheduler_heartbeat}

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
        arg_tags_filter = request.args.getlist('tags', None)

        if request.args.get('reset_tags') is not None:
            flask_session[FILTER_TAGS_COOKIE] = None
            arg_tags_filter = None
        else:
            cookie_val = flask_session.get(FILTER_TAGS_COOKIE)
            if arg_tags_filter:
                flask_session[FILTER_TAGS_COOKIE] = ','.join(arg_tags_filter)
            elif cookie_val:
                arg_tags_filter = cookie_val.split(',')

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
                "dag_import_error")

        from airflow.plugins_manager import import_errors as plugin_import_errors
        for filename, stacktrace in plugin_import_errors.items():
            flash(
                "Broken plugin: [{filename}] {stacktrace}".format(
                    stacktrace=stacktrace,
                    filename=filename),
                "error")

        # Get all the dag id the user could access
        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if arg_tags_filter:
            query = query.filter(DagModel.tags.any(DagTag.name.in_(arg_tags_filter)))

        if 'all_dags' not in filter_dag_ids:
            query = query.filter(DM.dag_id.in_(filter_dag_ids))

        start = current_page * dags_per_page
        end = start + dags_per_page

        dags = query.order_by(DagModel.dag_id).options(
            joinedload(DagModel.tags)).offset(start).limit(dags_per_page).all()
        tags = []

        dagtags = session.query(DagTag.name).distinct(DagTag.name).all()
        tags = [
            {"name": name, "selected": bool(arg_tags_filter and name in arg_tags_filter)}
            for name, in dagtags
        ]

        num_of_all_dags = query.count()
        num_of_pages = int(math.ceil(num_of_all_dags / float(dags_per_page)))

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
            num_dag_from=min(start + 1, num_of_all_dags),
            num_dag_to=min(end, num_of_all_dags),
            num_of_all_dags=num_of_all_dags,
            paging=wwwutils.generate_pages(current_page, num_of_pages,
                                           search=arg_search_query,
                                           showPaused=not hide_paused),
            num_runs=num_runs,
            tags=tags,
            user=user)

    @expose('/dag_stats', methods=['POST'])
    @has_access
    @provide_session
    def dag_stats(self, session=None):
        dr = models.DagRun

        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        dag_state_stats = session.query(dr.dag_id, dr.state, sqla.func.count(dr.state))\
            .group_by(dr.dag_id, dr.state)

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        payload = {}
        dag_state_stats = dag_state_stats.filter(dr.dag_id.in_(filter_dag_ids))
        data = {}

        for dag_id, state, count in dag_state_stats:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        for dag_id in filter_dag_ids:
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

    @expose('/task_stats', methods=['POST'])
    @has_access
    @provide_session
    def task_stats(self, session=None):
        TI = models.TaskInstance
        DagRun = models.DagRun
        Dag = models.DagModel
        allowed_dag_ids = set(appbuilder.sm.get_accessible_dag_ids())

        if not allowed_dag_ids:
            return wwwutils.json_response({})

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = {dag_id for dag_id, in session.query(models.DagModel.dag_id)}

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        LastDagRun = (
            session.query(
                        DagRun.dag_id,
                        sqla.func.max(DagRun.execution_date).label('execution_date'))
                   .join(Dag, Dag.dag_id == DagRun.dag_id)
                   .filter(DagRun.state != State.RUNNING)
                   .filter(Dag.is_active == True)  # noqa
                   .group_by(DagRun.dag_id)
        )

        RunningDagRun = (
            session.query(DagRun.dag_id, DagRun.execution_date)
                   .join(Dag, Dag.dag_id == DagRun.dag_id)
                   .filter(DagRun.state == State.RUNNING)
                   .filter(Dag.is_active == True)  # noqa
        )

        if selected_dag_ids:
            LastDagRun = LastDagRun.filter(DagRun.dag_id.in_(filter_dag_ids))
            RunningDagRun = RunningDagRun.filter(DagRun.dag_id.in_(filter_dag_ids))

        LastDagRun = LastDagRun.subquery('last_dag_run')
        RunningDagRun = RunningDagRun.subquery('running_dag_run')

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

        if selected_dag_ids:
            LastTI = LastTI.filter(TI.dag_id.in_(filter_dag_ids))
            RunningTI = RunningTI.filter(TI.dag_id.in_(filter_dag_ids))

        UnionTI = union_all(LastTI, RunningTI).alias('union_ti')

        qry = (
            session.query(UnionTI.c.dag_id, UnionTI.c.state, sqla.func.count())
                   .group_by(UnionTI.c.dag_id, UnionTI.c.state)
        )

        data = {}
        for dag_id, state, count in qry:
            if dag_id not in data:
                data[dag_id] = {}
            data[dag_id][state] = count

        payload = {}
        for dag_id in filter_dag_ids:
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

    @expose('/last_dagruns', methods=['POST'])
    @has_access
    @provide_session
    def last_dagruns(self, session=None):
        DagRun = models.DagRun

        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response({})

        query = session.query(
            DagRun.dag_id, sqla.func.max(DagRun.execution_date).label('last_run')
        ).group_by(DagRun.dag_id)

        # Filter to only ask for accessible and selected dags
        query = query.filter(DagRun.dag_id.in_(filter_dag_ids))

        resp = {
            r.dag_id.replace('.', '__dot__'): {
                'dag_id': r.dag_id,
                'last_run': r.last_run.isoformat(),
            } for r in query
        }
        return wwwutils.json_response(resp)

    @expose('/code')
    @has_dag_access(can_dag_read=True)
    @has_access
    @provide_session
    def code(self, session=None):
        all_errors = ""

        try:
            dag_id = request.args.get('dag_id')
            dag_orm = DagModel.get_dagmodel(dag_id, session=session)
            code = DagCode.get_code_by_fileloc(dag_orm.fileloc)
            html_code = highlight(
                code, lexers.PythonLexer(), HtmlFormatter(linenos=True))

        except Exception as e:
            all_errors += (
                "Exception encountered during " +
                "dag_id retrieval/dag retrieval fallback/code highlighting:\n\n{}\n".format(e)
            )
            html_code = '<p>Failed to load file.</p><p>Details: {}</p>'.format(
                escape(all_errors))

        return self.render_template(
            'airflow/dag_code.html', html_code=html_code, dag=dag_orm, title=dag_id,
            root=request.args.get('root'),
            demo_mode=conf.getboolean('webserver', 'demo_mode'),
            wrapped=conf.getboolean('webserver', 'default_wrap'))

    @expose('/dag_details')
    @has_dag_access(can_dag_read=True)
    @has_access
    @provide_session
    def dag_details(self, session=None):
        dag_id = request.args.get('dag_id')
        dag_orm = DagModel.get_dagmodel(dag_id, session=session)
        # FIXME: items needed for this view should move to the database
        dag = dag_orm.get_dag(STORE_SERIALIZED_DAGS)
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
    @provide_session
    def rendered(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        dttm = pendulum.parse(execution_date)
        form = DateTimeForm(data={'execution_date': dttm})
        root = request.args.get('root', '')

        logging.info("Retrieving rendered templates.")
        dag = dagbag.get_dag(dag_id)

        task = copy.copy(dag.get_task(task_id))
        ti = models.TaskInstance(task=task, execution_date=dttm)
        try:
            ti.get_rendered_template_fields()
        except AirflowException as e:
            msg = "Error rendering template: " + escape(e)
            if not PY2:
                if e.__cause__:
                    msg += Markup("<br/><br/>OriginalError: ") + escape(e.__cause__)
            flash(msg, "error")
        except Exception as e:
            flash("Error rendering template: " + str(e), "error")
        title = "Rendered Template"
        html_dict = {}
        for template_field in task.template_fields:
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

        def _get_logs_with_metadata(try_number, metadata):
            if ti is None:
                logs = ["*** Task instance did not exist in the DB\n"]
                metadata['end_of_log'] = True
            else:
                logs, metadatas = handler.read(ti, try_number, metadata=metadata)
                metadata = metadatas[0]
            return logs, metadata

        try:
            if ti is not None:
                dag = dagbag.get_dag(dag_id)
                ti.task = dag.get_task(ti.task_id)
            if response_format == 'json':
                logs, metadata = _get_logs_with_metadata(try_number, metadata)
                message = logs[0] if try_number is not None else logs
                return jsonify(message=message, metadata=metadata)

            filename_template = conf.get('core', 'LOG_FILENAME_TEMPLATE')
            attachment_filename = render_log_filename(
                ti=ti,
                try_number="all" if try_number is None else try_number,
                filename_template=filename_template)
            metadata['download_logs'] = True

            def _generate_log_stream(try_number, metadata):
                if try_number is None and ti is not None:
                    next_try = ti.next_try_number
                    try_numbers = list(range(1, next_try))
                else:
                    try_numbers = [try_number]
                for try_number in try_numbers:
                    metadata.pop('end_of_log', None)
                    metadata.pop('max_offset', None)
                    metadata.pop('offset', None)
                    while 'end_of_log' not in metadata or not metadata['end_of_log']:
                        logs, metadata = _get_logs_with_metadata(try_number, metadata)
                        yield "\n".join(logs) + "\n"
            return Response(_generate_log_stream(try_number, metadata),
                            mimetype="text/plain",
                            headers={"Content-Disposition": "attachment; filename={}".format(
                                attachment_filename)})
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
            root=root, wrapped=conf.getboolean('webserver', 'default_wrap'))

    @expose('/elasticsearch')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    @provide_session
    def elasticsearch(self, session=None):
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        try_number = request.args.get('try_number', 1)
        elasticsearch_frontend = conf.get('elasticsearch', 'frontend')
        log_id_template = conf.get('elasticsearch', 'log_id_template')
        log_id = log_id_template.format(
            dag_id=dag_id, task_id=task_id,
            execution_date=execution_date, try_number=try_number)
        url = 'https://' + elasticsearch_frontend.format(log_id=quote(log_id))
        return redirect(url)

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
        dep_context = DepContext(SCHEDULER_QUEUED_DEPS)
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

        # Make sure the task instance can be run
        dep_context = DepContext(
            deps=RUNNING_DEPS,
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

    @expose('/trigger', methods=['POST', 'GET'])
    @has_dag_access(can_dag_edit=True)
    @has_access
    @action_logging
    @provide_session
    def trigger(self, session=None):

        dag_id = request.values.get('dag_id')
        origin = request.values.get('origin') or url_for('Airflow.index')

        if request.method == 'GET':
            return self.render_template(
                'airflow/trigger.html',
                dag_id=dag_id,
                origin=origin,
                conf=''
            )

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
        conf = request.values.get('conf')
        if conf:
            try:
                run_conf = json.loads(conf)
            except ValueError:
                flash("Invalid JSON configuration", "error")
                return self.render_template(
                    'airflow/trigger.html',
                    dag_id=dag_id,
                    origin=origin,
                    conf=conf
                )

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
                       recursive=False, confirmed=False, only_failed=False):
        from airflow.exceptions import AirflowException

        if confirmed:
            count = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
            )

            flash("{0} task instances have been cleared".format(count))
            return redirect(origin)

        try:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=recursive,
                include_parentdag=recursive,
                only_failed=only_failed,
                dry_run=True,
            )
        except AirflowException as ex:
            flash(str(ex), 'error')
            return redirect(origin)

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
        only_failed = request.form.get('only_failed') == "true"

        dag = dag.sub_dag(
            task_regex=r"^{0}$".format(task_id),
            include_downstream=downstream,
            include_upstream=upstream)

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        return self._clear_dag_tis(dag, start_date, end_date, origin,
                                   recursive=recursive, confirmed=confirmed, only_failed=only_failed)

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

    @expose('/blocked', methods=['POST'])
    @has_access
    @provide_session
    def blocked(self, session=None):
        allowed_dag_ids = appbuilder.sm.get_accessible_dag_ids()

        if 'all_dags' in allowed_dag_ids:
            allowed_dag_ids = [dag_id for dag_id, in session.query(models.DagModel.dag_id)]

        # Filter by post parameters
        selected_dag_ids = {
            unquote(dag_id) for dag_id in request.form.getlist('dag_ids') if dag_id
        }

        if selected_dag_ids:
            filter_dag_ids = selected_dag_ids.intersection(allowed_dag_ids)
        else:
            filter_dag_ids = allowed_dag_ids

        if not filter_dag_ids:
            return wwwutils.json_response([])

        DR = models.DagRun

        dags = (
            session.query(DR.dag_id, sqla.func.count(DR.id))
                   .filter(DR.state == State.RUNNING)
               .filter(DR.dag_id.in_(filter_dag_ids))
                   .group_by(DR.dag_id)
        )

        payload = []
        for dag_id, active_dag_runs in dags:
            max_active_runs = 0
            dag = dagbag.get_dag(dag_id)
            if dag:
                # TODO: Make max_active_runs a column so we can query for it directly
                max_active_runs = dag.max_active_runs
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
            altered = set_state(tasks=[task], execution_date=execution_date,
                                upstream=upstream, downstream=downstream,
                                future=future, past=past, state=state,
                                commit=True)

            flash("Marked {} on {} task instances".format(state, len(altered)))
            return redirect(origin)

        to_be_altered = set_state(tasks=[task], execution_date=execution_date,
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
        upstream = request.form.get('failed_upstream') == "true"
        downstream = request.form.get('failed_downstream') == "true"
        future = request.form.get('failed_future') == "true"
        past = request.form.get('failed_past') == "true"

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
        upstream = request.form.get('success_upstream') == "true"
        downstream = request.form.get('success_downstream') == "true"
        future = request.form.get('success_future') == "true"
        past = request.form.get('success_past') == "true"

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
        dag_id = request.args.get('dag_id')
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if not dag:
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
        if num_runs:
            num_runs = int(num_runs)
        else:
            num_runs = conf.getint('webserver', 'default_dag_run_display_number')

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
            dr.execution_date: alchemy_to_dict(dr) for dr in dag_runs
        }

        dates = sorted(list(dag_runs.keys()))
        max_date = max(dates) if dates else None
        min_date = min(dates) if dates else None

        tis = dag.get_task_instances(
            start_date=min_date, end_date=base_date, session=session)
        task_instances = {}
        for ti in tis:
            task_instances[(ti.task_id, ti.execution_date)] = ti

        expanded = set()
        # The default recursion traces every path so that tree view has full
        # expand/collapse functionality. After 5,000 nodes we stop and fall
        # back on a quick DFS search for performance. See PR #320.
        node_count = [0]
        node_limit = 5000 / max(1, len(dag.leaves))

        def encode_ti(ti):
            if not ti:
                return None

            # NOTE: order of entry is important here because client JS relies on it for
            # tree node reconstruction. Remember to change JS code in tree.html
            # whenever order is altered.
            data = [
                ti.state,
                ti.try_number,
                None,  # start_ts
                None,  # duration
            ]

            if ti.start_date:
                # round to seconds to reduce payload size
                if six.PY2:
                    data[2] = int(pendulum.instance(ti.start_date).timestamp())
                else:
                    data[2] = int(ti.start_date.timestamp())
                if ti.duration is not None:
                    data[3] = int(ti.duration)

            return data

        def recurse_nodes(task, visited):
            node_count[0] += 1
            visited.add(task)
            task_id = task.task_id

            node = {
                'name': task.task_id,
                'instances': [
                    encode_ti(task_instances.get((task_id, d)))
                    for d in dates
                ],
                'num_dep': len(task.downstream_list),
                'operator': task.task_type,
                'retries': task.retries,
                'owner': task.owner,
                'ui_color': task.ui_color,
            }

            if task.downstream_list:
                children = [
                    recurse_nodes(t, visited) for t in task.downstream_list
                    if node_count[0] < node_limit or t not in visited]

                # D3 tree uses children vs _children to define what is
                # expanded or not. The following block makes it such that
                # repeated nodes are collapsed by default.
                if task.task_id not in expanded:
                    children_key = 'children'
                    expanded.add(task.task_id)
                else:
                    children_key = "_children"
                node[children_key] = children

            if task.depends_on_past:
                node['depends_on_past'] = task.depends_on_past
            if task.start_date:
                # round to seconds to reduce payload size
                if six.PY2:
                    node['start_ts'] = int(pendulum.instance(task.start_date).timestamp())
                else:
                    node['start_ts'] = int(task.start_date.timestamp())
                if task.end_date:
                    # round to seconds to reduce payload size
                    if six.PY2:
                        node['end_ts'] = int(pendulum.instance(task.end_date).timestamp())
                    else:
                        node['end_ts'] = int(task.end_date.timestamp())
            if task.extra_links:
                node['extra_links'] = task.extra_links
            if task.run_dag_id:
                node['run_dag_id'] = task.run_dag_id
            return node

        data = {
            'name': '[DAG]',
            'children': [recurse_nodes(t, set()) for t in dag.roots],
            'instances': [
                dag_runs.get(d) or {'execution_date': d.isoformat()}
                for d in dates
            ],
        }

        form = DateTimeWithNumRunsForm(data={'base_date': max_date,
                                             'num_runs': num_runs})
        external_logs = conf.get('elasticsearch', 'frontend')

        return self.render_template(
            'airflow/tree.html',
            operators=sorted({op.task_type: op for op in dag.tasks}.values(),
                             key=lambda x: x.task_type),
            root=root,
            form=form,
            dag=dag,
            # avoid spaces to reduce payload size
            data=htmlsafe_json_dumps(data, separators=(',', ':')),
            blur=blur, num_runs=num_runs,
            show_external_logs=bool(external_logs))

    @expose('/graph')
    @has_dag_access(can_dag_read=True)
    @has_access
    @gzipped
    @action_logging
    @provide_session
    def graph(self, session=None):
        dag_id = request.args.get('dag_id')
        # allow_tasks_actions = request.args.get('allow_tasks_actions', 'true') == 'true'
        blur = conf.getboolean('webserver', 'demo_mode')
        dag = dagbag.get_dag(dag_id)
        if not dag:
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

        def get_downstream(task):
            for t in task.downstream_list:
                edge = {
                    'source_id': task.task_id,
                    'target_id': t.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_downstream(t)

        for t in dag.roots:
            get_downstream(t)

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

        # NOTE: Special case when we don't want the actions to be
        # performed on dag runs made by DAG Operator.
        dagrun = dag.get_dagrun(execution_date=dttm)
        allow_tasks_actions = True
        if dagrun:
            allow_tasks_actions = str(dagrun.run_id).startswith(BackfillJob.ID_PREFIX_RUNDAG) is not True
        # print(allow_tasks_actions, dagrun.run_id)

        tasks = {
            t.task_id: {
                'dag_id': t.dag_id,
                'task_type': t.task_type,
                'extra_links': t.extra_links,
                'description': t.description,
                'run_dag_id': t.run_dag_id,
            }
            for t in dag.tasks}
        for task in task_instances:
            task_instances[task]['description'] = tasks[task]['description']
        if not tasks:
            flash("No tasks found", "error")
        session.commit()
        doc_md = markdown.markdown(dag.doc_md) \
            if hasattr(dag, 'doc_md') and dag.doc_md else ''

        external_logs = conf.get('elasticsearch', 'frontend')
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
            operators=sorted({op.task_type: op for op in dag.tasks}.values(),
                             key=lambda x: x.task_type),
            blur=blur,
            root=root or '',
            task_instances=task_instances,
            tasks=tasks,
            nodes=nodes,
            edges=edges,
            allow_tasks_actions=allow_tasks_actions,
            show_external_logs=bool(external_logs))

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
            # flash('DAG "{0}" seems to be missing.'.format(dag_id), "error")
            return make_response(('DAG not found!!', 200))

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

        def get_downstream(task):
            for t in task.downstream_list:
                edge = {
                    'source_id': task.task_id,
                    'target_id': t.task_id,
                }
                if edge not in edges:
                    edges.append(edge)
                    get_downstream(t)

        for t in dag.roots:
            get_downstream(t)

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
                'extra_links': t.extra_links,
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
            if tf.duration:
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
                # y value should reflect completed tries to have a 0 baseline.
                y.append(ti.prev_attempted_tries)
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
            task_id = task.task_id
            y[task_id] = []
            x[task_id] = []
            for ti in task.get_task_instances(start_date=min_date, end_date=base_date):
                ts = ti.execution_date
                if dag.schedule_interval and dag.following_schedule(ts):
                    ts = dag.following_schedule(ts)
                if ti.end_date:
                    dttm = wwwutils.epoch(ti.execution_date)
                    secs = (ti.end_date - ts).total_seconds()
                    x[task_id].append(dttm)
                    y[task_id].append(secs)

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
        dag_id = request.args.get('dag_id')
        is_paused = True if request.args.get('is_paused') == 'false' else False
        models.DagModel.get_dagmodel(dag_id).set_is_paused(
            is_paused=is_paused,
            store_serialized_dags=STORE_SERIALIZED_DAGS)
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

        dag = dagbag.get_dag(dag_id)
        # sync dag permission
        appbuilder.sm.sync_perm_for_dag(dag_id, dag.access_control)

        flash("DAG [{}] is now fresh as a daisy".format(dag_id))
        return redirect(request.referrer)

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
            if ti.start_date and ti.state]
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
            # prev_attempted_tries will reflect the currently running try_number
            # or the try_number of the last complete run
            # https://issues.apache.org/jira/browse/AIRFLOW-2143
            try_count = ti.prev_attempted_tries
            gantt_bar_items.append((ti.task_id, ti.start_date, end_date, ti.state, try_count))

        tf_count = 0
        try_count = 1
        prev_task_id = ""
        for tf in ti_fails:
            end_date = tf.end_date or timezone.utcnow()
            start_date = tf.start_date or end_date
            if tf_count != 0 and tf.task_id == prev_task_id:
                try_count = try_count + 1
            else:
                try_count = 1
            prev_task_id = tf.task_id
            gantt_bar_items.append((tf.task_id, start_date, end_date, State.FAILED, try_count))
            tf_count = tf_count + 1

        task_types = {}
        extra_links = {}
        for t in dag.tasks:
            task_types[t.task_id] = t.task_type
            extra_links[t.task_id] = t.extra_links

        tasks = []
        for gantt_bar_item in gantt_bar_items:
            task_id = gantt_bar_item[0]
            start_date = gantt_bar_item[1]
            end_date = gantt_bar_item[2]
            state = gantt_bar_item[3]
            try_count = gantt_bar_item[4]
            tasks.append({
                'startDate': wwwutils.epoch(start_date),
                'endDate': wwwutils.epoch(end_date),
                'isoStart': start_date.isoformat()[:-4],
                'isoEnd': end_date.isoformat()[:-4],
                'taskName': task_id,
                'taskType': task_types[ti.task_id],
                'duration': (end_date - start_date).total_seconds(),
                'status': state,
                'executionDate': dttm.isoformat(),
                'try_number': try_count,
                'extraLinks': extra_links[ti.task_id],
            })

        states = {task['status']: task['status'] for task in tasks}
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

    @expose('/extra_links')
    @has_dag_access(can_dag_read=True)
    @has_access
    @action_logging
    def extra_links(self):
        """
        A restful endpoint that returns external links for a given Operator

        It queries the operator that sent the request for the links it wishes
        to provide for a given external link name.

        API: GET
        Args: dag_id: The id of the dag containing the task in question
              task_id: The id of the task in question
              execution_date: The date of execution of the task
              link_name: The name of the link reference to find the actual URL for

        Returns:
            200: {url: <url of link>, error: None} - returned when there was no problem
                finding the URL
            404: {url: None, error: <error message>} - returned when the operator does
                not return a URL
        """
        dag_id = request.args.get('dag_id')
        task_id = request.args.get('task_id')
        execution_date = request.args.get('execution_date')
        link_name = request.args.get('link_name')
        dttm = airflow.utils.timezone.parse(execution_date)
        dag = dagbag.get_dag(dag_id)

        if not dag or task_id not in dag.task_ids:
            response = jsonify(
                {'url': None,
                 'error': "can't find dag {dag} or task_id {task_id}".format(
                     dag=dag,
                     task_id=task_id
                 )}
            )
            response.status_code = 404
            return response

        task = dag.get_task(task_id)

        try:
            url = task.get_extra_links(dttm, link_name)
        except ValueError as err:
            response = jsonify({'url': None, 'error': str(err)})
            response.status_code = 404
            return response
        if url:
            response = jsonify({'error': None, 'url': url})
            response.status_code = 200
            return response
        else:
            response = jsonify(
                {'url': None, 'error': 'No URL found for {dest}'.format(dest=link_name)})
            response.status_code = 404
            return response

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
    default_view = 'version'
    changelogs = None
    filepath = settings.CHANGELOG_PATH
    found_file = False

    try:
        with open (filepath, 'r') as f:
            changelogs = yaml.safe_load(f)
            found_file = True
    except IOError:
        pass

    @expose('/version')
    @has_access
    def version(self):
        return self.render_template(
            'airflow/version.html',
            found_file = self.found_file,
            changelogs = self.changelogs)

class ConfigurationView(AirflowBaseView):
    default_view = 'conf'

    @expose('/configuration')
    @has_access
    def conf(self):
        raw = request.args.get('raw') == "true"
        title = "Airflow Configuration"
        subtitle = configuration.AIRFLOW_CONFIG
        # Don't show config when expose_config variable is False in airflow config
        if conf.getboolean("webserver", "expose_config"):
            with open(configuration.AIRFLOW_CONFIG, 'r') as f:
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


class FileUploadBaseView(AirflowBaseView):
    '''Generic View for File Upload.'''
    __base_url = ''

    # NOTE: You can update the below attributes when subclassing this view.
    # set template name while using this generic view.
    default_view = 'list_view'
    template_name = 'airflow/file_upload_base.html'
    groups_template_name = 'airflow/file_upload_groups.html'
    # group refers to a config group.
    default_group = 'default'
    regex_valid_groupnames = re.compile('^[A-Za-z0-9_@()-]+$')
    accepted_file_extensions = ()
    fs_path = None   # the path in filesystem where the files should be saved.
    title = None
    files_editable = False

    # TODO: Update URL map a/c to http verbs instead of using path.
    # For ex: GET base_url/list should be GET base_url/
    # POST base_url/upload should be POST base_url/
    # GET base_url/download/filename should be GET base_url/filename
    # GET base_url/destroy/filename should be DELETE base_url/filename

    # TODO: Instead of using <path:pathname> use <our_own_converter:pathname>
    # That converter should allow only one level of subdirectory access.
    # https://exploreflask.com/en/latest/views.html#custom-converters
    urls_map = {
        'list_view': ["/".join(['base_url', 'list', '<path:pathname>']),
                      "/".join(['base_url', 'list', ''])],
        'upload_view': ["/".join(['base_url', 'upload', '<path:pathname>']),
                        "/".join(['base_url', 'upload', ''])],
        'download_view': ["/".join(['base_url', 'download', '<path:pathname>'])],
        'destroy_view': ["/".join(['base_url', 'destroy', '<path:pathname>'])],
        'edit_view': ["/".join(['base_url', 'edit', '<path:pathname>'])],
    }
    method_permission_name = {
        'list_view': 'access',
        'upload_view': 'access',
        'download_view': 'access',
        'destroy_view': 'access',
        'edit_view': 'access'
    }

    def __init__(self, *args, **kwargs):
        base_url = self.__class__.__name__
        self.__class__.list_view = copy.deepcopy(self.__class__.list_view)
        self.__class__.upload_view = copy.deepcopy(self.__class__.upload_view)
        self.__class__.download_view = copy.deepcopy(self.__class__.download_view)
        self.__class__.destroy_view = copy.deepcopy(self.__class__.destroy_view)
        self.__class__.edit_view = copy.deepcopy(self.__class__.edit_view)
        self.__class__.list_view._urls = []
        self.__class__.upload_view._urls = []
        self.__class__.download_view._urls = []
        self.__class__.destroy_view._urls = []
        self.__class__.edit_view._urls = []
        super().__init__(*args, **kwargs)

        for url in self.urls_map['list_view']:
            self.__class__.list_view._urls.append(
                (url.replace('base_url', base_url), ['GET']))

        for url in self.urls_map['upload_view']:
            self.__class__.upload_view._urls.append(
                (url.replace('base_url', base_url), ['POST']))

        for url in self.urls_map['download_view']:
            self.__class__.download_view._urls.append(
                (url.replace('base_url', base_url), ['GET']))

        for url in self.urls_map['destroy_view']:
            self.__class__.destroy_view._urls.append(
                (url.replace('base_url', base_url), ['GET']))

        for url in self.urls_map['edit_view']:
            self.__class__.edit_view._urls.append(
                (url.replace('base_url', base_url), ['GET', 'POST']))

        # self.__class__.list_view._urls.extend(
        #     (self.urls_map['list_view'].replace('base_url', base_url), ['GET']))

        # self.__class__.upload_view._urls.extend(
        #     (self.urls_map['upload_view'].replace('base_url', base_url), ['POST']))
        # self.__class__.download_view._urls.extend(
        #     (self.urls_map['download_view'].replace('base_url', base_url), ['GET']))
        # self.__class__.destroy_view._urls.extend(
        #     (self.urls_map['destroy_view'].replace('base_url', base_url), ['GET']))
        # self.__class__.edit_view._urls.extend(
        #     (self.urls_map['edit_view'].replace('base_url', base_url), ['GET', 'POST']))

        if self.fs_path:
            os.makedirs(self.fs_path, exist_ok=True)
            # os.makedirs(os.path.join(self.fs_path, self.default_group), exist_ok=True)

    @classmethod
    def get_base_url(cls):
        return cls.__base_url

    def get_file_path(self, pathname):
        self.check_attr_is_set(self.fs_path)
        if isinstance(pathname, str):
            path = os.path.join(self.fs_path, pathname)
        else:
            path = os.path.join(self.fs_path, *pathname)
        if self.path_valid(path):
            return path

    def path_valid(self, path):
        '''Path is valid if is inside `self.fs_path`
        '''
        norm_fs_path = os.path.normpath(self.fs_path)
        norm_path = os.path.normpath(os.path.join(self.fs_path, path))
        if norm_fs_path == norm_path[:len(norm_fs_path)]:
            return True
        raise Exception('Illegal Access to other directories not allowed.')

    def check_attr_is_set(self, *args):
        # TODO: Refactor this to raise more appropriate error messages.
        for attr in args:
            if not attr:
                raise Exception('All arguments not set. Please set them to appropriate value')

    def on_save_complete(self):
        '''Called when all files uploaded are saved'''
        pass

    def action_logger(f):
        '''
        Decorator to log user actions.

        Similar to decorator `action_logging` in `decorators.py` but it
        logs `f.__class__.__name__` + `f.__name__` as event.
        '''
        @functools.wraps(f)
        def wrapper(*args, **kwargs):

            try:
                with create_session() as session:
                    if g.user.is_anonymous:
                        user = 'anonymous'
                    else:
                        user = g.user.username

                    log = Log(
                        event="{}.{}".format(args[0].__class__.__name__, f.__name__)[:30],
                        task_instance=None,
                        owner=user,
                        extra=str(list(request.args.items())),
                        task_id=request.args.get('task_id'),
                        dag_id=request.args.get('dag_id'),
                        source_ip=request.environ['REMOTE_ADDR'])

                    if 'execution_date' in request.args:
                        log.execution_date = pendulum.parse(
                            request.args.get('execution_date'))

                    session.add(log)

                return f(*args, **kwargs)
            except Exception as e:
                print(e)

        return wrapper

    def extra_files(self, files):
        # use this view to add other objects to file dict here.
        return files

    @has_access
    @action_logger
    def list_view(self, pathname=None):
        self.check_attr_is_set(self.fs_path, self.accepted_file_extensions)
        if pathname:
            path = self.get_file_path(pathname)
        else:
            path = self.fs_path
            # pathname = ''  # required for path concatenation in file_upload_base.html
        files = self.get_details(path, self.accepted_file_extensions)
        files = self.extra_files(files)
        return self.render_template(
            self.template_name,
            files=files,
            view=self.__class__.__name__,
            accepted_file_extensions=self.accepted_file_extensions,
            title=self.title,
            files_editable=self.files_editable,
            pathname=pathname
        )

    def extra_work_after_file_save(self, filedest, *args, **kwargs):
        return

    @has_access
    @action_logger
    def upload_view(self, pathname=None):
        list_files = request.files.getlist("file")
        files_uploaded = 0
        for upload in list_files:
            filename = upload.filename
            if filename.endswith(self.accepted_file_extensions):
                if pathname:
                    destination = self.get_file_path([pathname, filename])
                else:
                    destination = self.get_file_path(filename)
                upload.save(destination)
                # print("Extra work")
                self.extra_work_after_file_save(destination)
                # print("Extra work done")
                try:
                    AirflowBaseView.audit_logging(
                        "{}.{}".format(self.__class__.__name__, 'upload_view'),
                        filename, request.environ['REMOTE_ADDR'])
                except Exception:
                    pass
                files_uploaded += 1
            else:
                flash('File, ' + filename + ' not allowed', 'error')
        if files_uploaded:
            self.flash_on_upload_done(files_uploaded)
        self.on_save_complete()
        return redirect(url_for(self.__class__.__name__ + '.list_view', pathname=pathname))

    def flash_on_upload_done(self, files_uploaded):
        flash(str(files_uploaded) + ' files uploaded!!', 'success')

    @has_access
    @action_logger
    def edit_view(self, pathname):
        '''When `self.files_editable` is set to True, you should override this view'''
        return make_response(('BAD_REQUEST', 400))
        # raise NotImplementedError('Please implement this in your subclass to be able to edit files.')

    @has_access
    @action_logger
    def download_view(self, pathname):
        file_path = self.get_file_path(pathname)
        AirflowBaseView.audit_logging(
            "{}.{}".format(self.__class__.__name__, 'download_view'),
            pathname, request.environ['REMOTE_ADDR'])
        return send_file(file_path, as_attachment=True, conditional=True)

    @has_access
    def destroy_view(self, pathname):
        file = Path(self.get_file_path(pathname))
        print(file)
        if file.exists():
            file.unlink()
            AirflowBaseView.audit_logging(
                "{}.{}".format(self.__class__.__name__, 'destroy_view'),
                pathname, request.environ['REMOTE_ADDR'])
            flash('File ' + pathname + ' successfully deleted.', category='warning')
        else:
            flash('File ' + pathname + ' not found.', category='error')

        redirect_pathname = Path(pathname).parent.stem
        return redirect(url_for(self.__class__.__name__ + '.list_view', pathname=redirect_pathname))


class SparkDepView(FileUploadBaseView):
    fs_path = settings.SPARK_DEPENDENCIES_FOLDER
    accepted_file_extensions = ('.jar', '.egg', '.zip', '.py')
    title = 'Spark Dependencies'

    def on_save_complete(self):
        flash('To include the file(s) for spark job, select them from spark configuration.', 'success')


class CodeArtifactView(FileUploadBaseView):
    fs_path = settings.CODE_ARTIFACTS_FOLDER
    accepted_file_extensions = ('.jar', '.egg', '.zip', '.py')
    title = 'Code Artifacts'
    class_permission_name = 'Code Artifacts'


class TrainedModelsView(FileUploadBaseView):
    base_fs_path = settings.MODEL_SERVERS
    temp_fs_path = os.path.join(settings.MODEL_SERVERS, 'tmp')

    fs_path = settings.MODEL_SERVERS
    # accepted_file_extensions = ('.tar', '.tar.gz')
    accepted_file_extensions = ('',)
    title = 'Pre-trained models and dataset repositories'
    class_permission_name = 'Models and Datasets'
    method_permission_name = {
        'list_view': 'access',
        'upload_view': 'access',
        'download_view': 'access',
        'destroy_view': 'access',
        'edit_view': 'access'
    }
    template_name = 'airflow/tf_file_upload_base.html'

    fs_mapping = {
        'tf_models': {
            'path': os.path.join(base_fs_path, 'tf-models'),
            'extract_on_upload': True,
            'update_config': True,
            'accept_extensions': ('.tar', '.gz')
        },
        'spark_models': {
            'path': os.path.join(base_fs_path, 'spark-models'),
            'extract_on_upload': False,
            'update_config': False,
            'accept_extensions': ('.tar', '.gz')
        },
        'pytorch_models': {
            'path': os.path.join(base_fs_path, 'pytorch-models'),
            'extract_on_upload': False,
            'update_config': True,
            'accept_extensions': ('.mar',)
        },
        'other_models': {
            'path': os.path.join(base_fs_path, 'other-models'),
            'extract_on_upload': True,
            'update_config': False,
            'accept_extensions': ('.tar', '.gz')
        },
        'datasets': {
            'path': os.path.join(base_fs_path, 'datasets'),
            'extract_on_upload': True,
            'update_config': False,
            'accept_extensions': ('.tar', '.gz')
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        for key, val in self.fs_mapping.items():
            os.makedirs(val['path'], exist_ok=True)

    def flash_on_upload_done(self, files_uploaded):
        return

    @has_access
    def list_view(self, pathname=None):
        # THIS view should return the same content irrespective of pathname
        if pathname:
            return redirect(url_for(self.__class__.__name__ + '.list_view', pathname=''))

        files = {}
        for key, val in self.fs_mapping.items():
            # print(key, val)
            files[key] = {}
            pth = val['path']
            # print(pth)
            files[key] = self.get_details(pth, ('', ))
            files[key] = self.extra_files(files[key], pth)
            # print("KEY====>>>>", key, files[key])
        # for i in files:
        #     print(files[i])
        return self.render_template(
            self.template_name,
            files=files,
            view=self.__class__.__name__,
            title=self.title,
            files_editable=False,
            pathname=None,
            fs_mapping=self.fs_mapping,
            max_chunk_size=settings.MAX_CHUNK_SIZE,
            max_file_size=settings.MAX_FILE_SIZE
        )

    @csrf.exempt
    @has_access
    def upload_view(self, pathname=None):
        # print(request.form)
        file = request.files['file']
        temp_save_path = os.path.join(self.temp_fs_path, request.form['dzuuid'])
        current_chunk = int(request.form['dzchunkindex'])

        def create_chunks_folder():
            os.makedirs(temp_save_path, exist_ok=True)

        create_chunks_folder()
        logging.info('{} chunk recieved, chunk index {}'.format(
            file.filename,
            current_chunk)
        )

        # save the chunk using its index
        # TODO: check if lock is required here.
        with open(os.path.join(temp_save_path, str(current_chunk)), 'wb') as f:
            f.write(file.stream.read())

        return make_response(('Chunk {} saved.'.format(current_chunk), 200))

    @has_access
    # @action_logging
    @expose('/TrainedModelsView/extract/', methods=['POST'])
    @expose('/TrainedModelsView/extract/<path:pathname>', methods=['POST'])
    @csrf.exempt
    def extract_view(self, pathname=None):
        # return make_response(('Thread to combine files has started.', 400))
        temp_save_path = os.path.join(self.temp_fs_path, request.form['dzuuid'])
        file = request.form['file']
        total_chunks = int(request.form['totalChunkCount'])
        # print(pathname)
        AirflowBaseView.audit_logging(
            'TrainedModelsView.extract',
            file,
            request.environ['REMOTE_ADDR'])
        remote_addr = str(request.environ['REMOTE_ADDR'])
        # print("RMEOTE ADDR" + remote_addr)
        combine_chunks_thread = threading.Thread(
            target=self.extra_work_after_file_save,
            args=(temp_save_path, file, total_chunks, pathname, remote_addr))
        combine_chunks_thread.start()
        return make_response(('Thread to combine files has started.', 200))

    def extra_files(self, files, fs_path=None):
        '''
        This method adds extra files which need to be shown, & removes previously added
        files which needn't be shown in the UI.
        '''
        # print(fs_path)
        if not fs_path:
            fs_path = self.fs_path
        # also removing `models.config` which we don't want to show
        dirs = [name for name in os.listdir(fs_path) if os.path.isdir(os.path.join(fs_path, name))]
        # print("Dirs ===>>>", dirs)
        for d in dirs:
            fileStatsObj = os.stat(os.path.join(fs_path, d))
            modificationTime = time.ctime(fileStatsObj[stat.ST_MTIME])  # get last modified time
            size_bytes = fileStatsObj.st_size
            size = AirflowBaseView.convert_size(size_bytes)
            temp_dict = {'time': modificationTime.split(' ', 1)[1], 'size': size, 'dir': True}
            files[d] = temp_dict
        # remove models.config
        for file in list(files.keys()):
            if file.endswith('.config'):
                files.pop(file, None)

        return files

    def set_config(self, config, pathname):
        try:
            with open(os.path.join(pathname, 'models.config'), 'w') as f:
                f.write(config)
        except Exception as e:
            print(e)

    def get_file_path(self, pathname):
        if isinstance(pathname, list):
            fs_path = self.fs_mapping.get(pathname[0], self.fs_mapping['other_models'])['path']
            # print("FS_PATh ====>", fs_path)
            return os.path.join(fs_path, pathname[1])
        else:
            return os.path.join(self.fs_path, pathname)

    def update_models_config(self, pathname, model_type, file=None, delete=False, remote_addr=None):
        if model_type == 'tf_models':
            config = 'model_config_list: {'
            dirs = [name for name in os.listdir(pathname) if os.path.isdir(os.path.join(pathname, name))]
            for dname in dirs:
                # print(dname)
                config += '''config: {
                                name: "%s",
                                base_path: "/usr/local/couture/trained-models/%s/%s",
                                model_platform: "tensorflow"
                            },''' % (dname, 'tf-models', dname)
            config += '}'
            # flash('All Model servers have been updated')
            self.set_config(config, pathname)
            # ERROR: (1406, "Data too long for column 'event' at row 1")
            # AirflowBaseView.audit_logging(
            #     'TrainedModelsView.update_models_config',
            #     extra='tf-models',
            #     source_ip=remote_addr)
        elif model_type == 'pytorch_models':
            # add/update model to serving
            model_name = str(Path(file).stem)
            # try:
            logging.info(f'Pytorch URL: {settings.PYTORCH_MANAGEMENT_URL}/models')
            if not delete:
                requests.post(f'{settings.PYTORCH_MANAGEMENT_URL}/models',
                    params={
                        'url': file,
                        'model_name': model_name
                    })
                # Have at least 1 worker running!
                # https://github.com/pytorch/serve/blob/master/docs/management_api.md#scale-workers
                requests.put(f'{settings.PYTORCH_MANAGEMENT_URL}/models/{model_name}')

            else:
                # TODO: unregister model here
                pass
            # except Exception as e:
            #     logging.error(str(e))
            # ERROR: (1406, "Data too long for column 'event' at row 1")
            # AirflowBaseView.audit_logging(
            #     'TrainedModelsView.update_models_config',
            #     extra='pytorch-models',
            #     source_ip=remote_addr)

    def extra_work_after_file_save(self, temp_save_path, file, total_chunks, pathname, remote_addr=None):
        # TODO: MODIFY THIS HACK.
        fs_key = pathname
        fs_path = self.fs_mapping[fs_key]['path']
        pathname = os.path.join(fs_path, file)
        # TODO: check if lock or semaphore is required here.

        # assuming all chunks exists:
        with open(pathname, 'wb') as f:
            for chunk_index in range(0, total_chunks):
                with open(os.path.join(temp_save_path, str(chunk_index)), 'rb') as chunk:
                    f.write(chunk.read())

        # delete the temp files
        shutil.rmtree(temp_save_path)

        logging.info(f'Final Size of file {file}: {os.path.getsize(pathname)}')

        if not self.fs_mapping[fs_key]['extract_on_upload'] and not self.fs_mapping[fs_key]['update_config']:
            return

        if str(pathname).endswith(('.mar', )) and self.fs_mapping[fs_key]['update_config']:
                self.update_models_config(fs_path, fs_key, file, remote_addr=remote_addr)

        if str(pathname).endswith(('.tar', '.tar.gz')):
            file_path = self.get_file_path(pathname)
            with tarfile.open(Path(file_path)) as tar:
                for content in tar:
                    try:
                        tar.extract(content, path=fs_path)
                    except Exception as e:
                        print(e)
                        existing_content = Path(fs_path).joinpath(content)
                        if existing_content.is_dir():
                            shutil.rmtree(existing_content)
                        else:
                            existing_content.unlink()
                        tar.extract(content, path=fs_path)
                        flash('{} already exists, overwriting'.format(content), category='warning')

                        # # after extracting, update models.config
                        # if existing_content.isdir():
                    finally:
                        content_path = Path(fs_path).joinpath(content.name)
                        os.chmod(content_path, content.mode)
            # flash('Extracted {}'.format(pathname))
            Path(str(pathname)).unlink()
            if self.fs_mapping[fs_key]['update_config']:
                self.update_models_config(fs_path, fs_key, remote_addr=remote_addr)

    @has_access
    # @action_logger
    def download_view(self, pathname):
        file_path = self.get_file_path(str(pathname).split('/'))
        arcname = pathname.split('/')[-1]
        if os.path.isdir(file_path):
            f = tempfile.SpooledTemporaryFile(suffix='.tar.gz')
            with tarfile.open(fileobj=f, mode='w:gz') as tar:
                tar.add(file_path, arcname=arcname)

            f.flush()
            f.seek(0)
            # ERROR: (1406, "Data too long for column 'event' at row 1")
            # AirflowBaseView.audit_logging(
            #     'TrainedModelsView.download_view',
            #     arcname,
            #     request.environ['REMOTE_ADDR'])
            return send_file(f,
                             as_attachment=True,
                             conditional=True,
                             attachment_filename='{}.tar.gz'.format(pathname),
                             mimetype='application/gzip')
        return send_file(file_path, as_attachment=True, conditional=True)

    @has_access
    def destroy_view(self, pathname):
        file = Path(self.get_file_path(str(pathname).split('/')))
        if file.exists() and file.is_file():
            file.unlink()
            flash('File ' + pathname + ' successfully deleted.', category='warning')
        elif file.exists() and file.is_dir():
            shutil.rmtree(file)
            flash('Folder ' + pathname + ' successfully deleted.', category='warning')
            pth = pathname.split('/')[0]
            if self.fs_mapping[pth]['update_config']:
                self.update_models_config(self.fs_mapping[pth]['path'],
                    pth, delete=True, remote_addr=str(request.environ['REMOTE_ADDR']))
        else:
            flash('File/Folder ' + pathname + ' not found.', category='error')
        return redirect(url_for(self.__class__.__name__ + '.list_view', pathname=''))


class HadoopConfView(FileUploadBaseView):
    # TODO: Merge this with file upload baseview.
    default_view = 'groups_view'
    fs_path = settings.HADOOP_CONFIGS_FOLDER
    accepted_file_extensions = ('.xml', )
    title = 'Spark Hadoop Config Groups'
    files_editable = True
    groups_template_name = 'airflow/hadoop_upload_groups.html'

    def get_default_group_path(self):
        try:
            default_group_p = os.readlink(os.path.join(self.fs_path, self.default_group))
        except FileNotFoundError:
            default_group_p = ""
        if not os.path.isabs(default_group_p):
            default_group_p = os.path.join(self.fs_path, default_group_p)
        default_group_p = os.path.normpath(default_group_p)
        return default_group_p

    def get_groups(self):
        groups = []
        default_group_name = None
        default_group_p = self.get_default_group_path()
        for f in os.scandir(self.fs_path):
            if f.is_dir() and f.name != self.default_group:
                if os.path.normpath(f.path) != default_group_p:
                    groups.append([f, False])
                else:
                    groups.append([f, True])
                    default_group_name = f.name
        return groups, default_group_name

    def _change_default_group(self, groupname):
        try:
            try:
                os.remove(os.path.join(self.fs_path, self.default_group))
            except OSError:
                pass
            norm_group_path = os.path.normpath(os.path.join(self.fs_path, groupname))
            # Changing putting relative symlink here
            os.symlink(os.path.relpath(norm_group_path, self.fs_path),
                       os.path.join(self.fs_path, self.default_group))
            # os.symlink(norm_group_path,
            #            os.path.join(self.fs_path, self.default_group))
            AirflowBaseView.audit_logging(
                "{}.{}".format(self.__class__.__name__, 'change_default_group'),
                groupname, request.environ['REMOTE_ADDR'])
        except Exception:
            # print(e, type(e))
            pass

    @expose('/HadoopConfView/change-default-group/', methods=['GET'])
    @has_access
    @FileUploadBaseView.action_logger
    def change_default_group(self):
        groupname = request.args.get('group')
        self._change_default_group(groupname)
        return redirect(url_for('HadoopConfView.groups_view'))

    @expose('/HadoopConfView/delete-group/<string:groupname>', methods=['GET'])
    @has_access
    @FileUploadBaseView.action_logger
    def delete_group(self, groupname):
        default_group_p = self.get_default_group_path()
        norm_group_path = os.path.normpath(os.path.join(self.fs_path, groupname))
        if default_group_p == norm_group_path:
            flash('Cannot delete the default group. Change default group first.', category='warning')
        else:
            shutil.rmtree(norm_group_path, ignore_errors=True)
            AirflowBaseView.audit_logging(
                "{}.{}".format(self.__class__.__name__, 'delete_group'),
                groupname, request.environ['REMOTE_ADDR'])
        return redirect(url_for('HadoopConfView.groups_view'))

    @expose('/HadoopConfView/groups/', methods=['GET', 'POST'])
    @has_access
    @FileUploadBaseView.action_logger
    def groups_view(self):
        groups = []
        default_group_name = None
        if request.method == 'GET':
            if not os.path.islink(os.path.join(self.fs_path, self.default_group)):
                flash('No Default Hadoop Config Group set', category='warning')
            groups, default_group_name = self.get_groups()
            return self.render_template(
                self.groups_template_name,
                groups=groups,
                view=self.__class__.__name__,
                accepted_file_extensions=self.accepted_file_extensions,
                title=self.title,
                default_group=default_group_name
            )
        else:
            name = request.form.get('name')
            if name and self.regex_valid_groupnames.match(name):
                os.makedirs(os.path.join(self.fs_path, name), exist_ok=True)
                # copying default spark config.
                shutil.copyfile(settings.SAMPLE_SPARK_CONF_PATH, os.path.join(self.fs_path, *[name, 'couture-spark.conf']))
                groups, _ = self.get_groups()
                if len(groups) <= 1:
                    self._change_default_group(name)
                    # making this the default group.
                flash('Group added !', category='success')
                AirflowBaseView.audit_logging(
                    "{}.{}".format(self.__class__.__name__, 'add_group'),
                    name, request.environ['REMOTE_ADDR'])
            else:
                flash('Invalid group name provided !', category='error')
            return redirect(url_for('HadoopConfView.groups_view'))

    @has_access
    @action_logging
    def edit_view(self, pathname):
        from lxml import etree as ET
        UPLOAD_FOLDER = self.fs_path
        if request.method == 'GET':
            xml_file = os.path.join(UPLOAD_FOLDER, pathname)
            self.path_valid(xml_file)
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
                                        pathname=pathname)

        if request.method == 'POST':
            xml_file = os.path.join(UPLOAD_FOLDER, pathname)
            self.path_valid(xml_file)
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

            for key in request.form:
                if key.startswith('new-config-key-') and request.form[key]:
                    key_no = key.split('-')[-1]
                    prop = ET.Element("property")
                    root.append(prop)
                    nm = ET.SubElement(prop, "name")
                    nm.text = request.form[key]
                    val = ET.SubElement(prop, "value")
                    val.text = request.form['new-config-value-' + key_no]

            del_name = request.form.get('option_title_config_delete')  # for deleting a property from file
            if del_name:
                for p in root.iter('property'):
                    n = p.find('name').text
                    if n == del_name:
                        root.remove(p)

            tree.write(xml_file)  # writing all the updated changes to the fields

            return redirect(url_for('HadoopConfView.edit_view', pathname=pathname))


class ExportConfigsView(AirflowBaseView, BaseApi):
    _configs_path = [
        settings.HADOOP_CONFIGS_FOLDER,
        # settings.SPARK_CONF_PATH,
    ]

    _dependencies_path = [
        settings.SPARK_DEPENDENCIES_FOLDER,
    ]

    _dags_path = [
        settings.DAGS_FOLDER
    ]

    @staticmethod
    def filter_files(tarinfo):
        # TODO: Filter files here.
        return tarinfo
        # if str(tarinfo.name).endswith('.py') .....

    @staticmethod
    def moveTree(sourceRoot, destRoot):
        # COPY whole dirs(only) present in sourceRoot.
        if not os.path.exists(destRoot):
            return False
        dirs = [name for name in os.listdir(sourceRoot) if os.path.isdir(os.path.join(sourceRoot, name))]
        for folder in dirs:
            if not os.path.islink(os.path.join(destRoot, folder)):
                shutil.rmtree(os.path.join(destRoot, folder), ignore_errors=True)
            else:
                os.unlink(os.path.join(destRoot, folder))

            # else:
                # print("folder:::", folder)
            if os.path.islink(os.path.join(sourceRoot, folder)):
                os.symlink(os.readlink(os.path.join(sourceRoot, folder)), os.path.join(destRoot, folder))
            else:
                shutil.copytree(os.path.join(sourceRoot, folder),
                                os.path.join(destRoot, folder),
                                copy_function=shutil.copy,
                                symlinks=True)

        files = [name for name in os.listdir(sourceRoot) if os.path.isfile(os.path.join(sourceRoot, name))]
        for file in files:
            shutil.copy(os.path.join(sourceRoot, file),
                        os.path.join(destRoot, file))

    def export(self, export_paths, name='configs'):
        f = tempfile.SpooledTemporaryFile(suffix='.tar.gz')
        airflow_home_parent = os.path.normpath(os.path.join(AIRFLOW_HOME, os.pardir))
        root_dir = Path(name)
        with tarfile.open(fileobj=f, mode='w:gz') as tar:
            for path in export_paths:
                try:
                    tar.add(path,
                            arcname=str(root_dir.joinpath(Path(path).relative_to(airflow_home_parent))),
                            filter=self.filter_files)
                except FileNotFoundError:
                    pass

        f.flush()
        f.seek(0)
        return send_file(f,
                         as_attachment=True,
                         conditional=True,
                         attachment_filename='{}.tar.gz'.format(name),
                         mimetype='application/gzip')

    def imprt(self, import_paths, tar):
        # NOTE: All folder imports are from AIRFLOW_HOME/../
        # The sent files should be overwritten and the rest files already
        # present in the filesystem should be preserved.
        if not tar:
            return self.response_400('Missing "sources"')
        airflow_home_parent = Path(AIRFLOW_HOME).parent
        with tempfile.TemporaryDirectory() as tmpdir:
            tarfile.open(fileobj=tar, mode='r:gz').extractall(path=tmpdir)
            contents = [name for name in os.listdir(tmpdir) if os.path.isdir(os.path.join(tmpdir, name))]
            if not contents:
                return self.response_400(
                    message="No root directory found. Keep a root directory.")
            elif len(contents) > 1:
                return self.response_400(
                    message="Multiple directories in root found while extracting.\
                            Keep only one root directory.")
            # go inside the first directory.
            root_dir = Path(contents[0])
            for path in import_paths:
                path_in_tempdir = os.path.join(tmpdir,
                                               *[root_dir, Path(path).relative_to(airflow_home_parent)])
                if os.path.isfile(path) and os.path.exists(path_in_tempdir):
                    shutil.copyfile(path_in_tempdir, path, follow_symlinks=True)
                elif os.path.isdir(path) and os.path.exists(path_in_tempdir):
                    self.moveTree(path_in_tempdir, path)

        return self.response(200, message="OK")

    @expose('/configs/', methods=['GET', 'POST'])
    # @protect(allow_browser_login=True)
    @csrf.exempt
    def configs(self):
        # TODO: enable authentication.
        if request.method == 'GET':
            return self.export(self._configs_path)
        if request.method == 'POST':
            return self.imprt(self._configs_path, tar=request.files.get('sources'))

    @expose('/dependencies/', methods=['GET', 'POST'])
    # @protect(allow_browser_login=True)
    @csrf.exempt
    def dependencies(self):
        # TODO: enable authentication.
        if request.method == 'GET':
            return self.export(self._dependencies_path, name='dependencies')
        if request.method == 'POST':
            return self.imprt(self._dependencies_path, tar=request.files.get('sources'))

    @expose('/dags/', methods=['GET', 'POST'])
    # @protect(allow_browser_login=True)
    @csrf.exempt
    def dags(self):
        # TODO: enable authentication.
        if request.method == 'GET':
            return self.export(self._dags_path, name='dags')
        if request.method == 'POST':
            return self.imprt(self._dags_path, tar=request.files.get('sources'))


class EDAView(AirflowBaseView, BaseApi):
    default_view = 'source_view'
    output_path = os.path.join(settings.EDA_HOME, *['outputs'])
    temp_save_path = os.path.join(settings.EDA_HOME, *['tmp'])
    hdfs_path = '/data/eda/raw/inputfiles/'
    sources_key = 'EDA_Sources'
    class_permission_name = 'Exploratory data analysis'
    method_permission_name = {
        'source_view': 'access',
        'source_destroy_view': 'access',
        'list_outputs_view': 'access',
        'dashboard_view': 'access'
    }

    def __init__(self, *args, **kwargs):
        os.makedirs(self.output_path, exist_ok=True)
        os.makedirs(self.temp_save_path, exist_ok=True)
        super().__init__(*args, **kwargs)

    def get_sources(self):
        return models.EdaSource.all_sources()

    def get_outputs(self):
        outputs_folder = Path(self.output_path)
        # Use os.scandir() instead ?
        tree = []
        curr = 0
        dir_node_id = {}
        for dirpath, dirs, files in os.walk(self.output_path):
            root = Path(dirpath).stem
            tree.append({'id': 'node-{}'.format(curr),
                         'state': {
                             'opened': 'true'},
                         'parent': '#' if not curr else dir_node_id[Path(dirpath).parts[-2]],
                         'text': root})
            dir_node_id[root] = 'node-{}'.format(curr)
            curr += 1
            for file in files:
                if file.endswith(('.htm', '.html')):
                    fpath = Path(os.path.join(dirpath, file))
                    tree.append({'id': 'node-{}'.format(curr),
                                 'parent': dir_node_id[root],
                                 'text': file,
                                 'type': 'dashboard',
                                 'a_attr': {
                                     'href': url_for('EDAView.dashboard_view',
                                                     filename=fpath.relative_to(outputs_folder))
                    }})
                    curr += 1
        return tree

    def add_source(self, conn_uri, source_type, tablename=None):
        source_type = models.EdaSourcesEnum(source_type)
        models.EdaSource.add_source(conn_uri, source_type, tablename)
        # sources = set(self.get_sources())
        # sources.add(source)
        # sources = list(sources)
        # models.Variable.set(self.sources_key, sources, serialize_json=True)

    def remove_source(self, source):
        models.EdaSource.remove_source(source)

    @expose('/eda/sources/', methods=['GET', 'POST'])
    @has_access
    @csrf.exempt
    @action_logging
    def source_view(self):
        if request.method == "GET":
            sources = self.get_sources()
            outputs = json.dumps(self.get_outputs())
            return self.render_template('eda/eda_sources.html',
                                        sources=sources,
                                        source_types=models.EdaSourcesEnum,
                                        outputs=outputs)
        conn_uri = request.form.get('path')
        if not conn_uri and not request.files.get('file'):
            flash('Invalid source.', category='warning')
        elif request.form.get('type') == models.EdaSourcesEnum.hdfs.value:
            self.add_source(conn_uri, source_type=models.EdaSourcesEnum.hdfs.value)
            flash('Source Added.')
        else:
            # csv file. Trigger an run immdeiately.
            file = request.files.get('file')
            # dest = os.path.join(self.temp_save_path, file.filename)
            # file.save(dest)
            # self.add_source_file(file)
            dest = os.path.join(self.temp_save_path, file.filename)
            file.save(dest)
            # add_source_file_thread = threading.Thread(
            #     target=self.add_source_file,
            #     args=(dest, self.hdfs_path))
            # add_source_file_thread.start()
            # flash(f'File will be copied to HDFS shortly. \
            #     Trigger EDA on this file at {self.hdfs_path}{file.filename}.')
            # move data to hdfs

            # just mock the eda source
            eda_source = models.EdaSource(connection_uri=dest, source_type='local')
            dag_ids = self.create_eda_dags(eda_source, runtype='L0')
            _parse_dags(update_DagModel=True)

            for dag_id in dag_ids:
                unpause_dag(dag_id)
        return redirect(url_for('EDAView.source_view'))

    # def add_source_file(self, file, hdfs_path):

    #     hdfs_fileloc = move_to_hdfs(file, hdfs_path)
    #     # move_to_hdfs_thread = threading.Thread(
    #     #         target=move_to_hdfs,
    #     #         args=(os.path.join(self.temp_save_path, file.filename)))
    #     # move_to_hdfs_thread.start()

    #     # TODO: This is done because of the way the hdfs path is handled in EDA DAGs.
    #     # Check if it is the correct way.
    #     if str(hdfs_fileloc).startswith('/'):
    #         hdfs_fileloc = 'hdfs:/' + hdfs_fileloc
    #     else:
    #         hdfs_fileloc = 'hdfs://' + hdfs_fileloc
    #     # print(os.stat(dest))
    #     self.add_source(hdfs_fileloc, source_type=models.EdaSourcesEnum.hdfs.value)

    def create_eda_dags(self, eda_source, runtype):

        username = g.user.username
        now = datetime.now()

        if runtype == 'L0':
            eda_type = 'preliminary'
            eda_dag_id_prefix = 'EDAPreliminaryDataSummary'
            viz_dag_id_prefix = 'EDAPreliminaryVisualisation'
            eda_dag_template = 'dags/default_EDA_preliminary_data_summary.jinja2'
            viz_dag_template = 'dags/default_EDA_preliminary_visualisations.jinja2'
        else:
            # L1 type EDA
            eda_type = 'data_importance'
            eda_dag_id_prefix = 'EDADataScoreAndFeatureSummary'
            viz_dag_id_prefix = 'EDADataScoreAndFeatureVisualisation'
            eda_dag_template = 'dags/default_EDA_datascore_and_feature_summary.jinja2'
            viz_dag_template = 'dags/default_EDA_datascore_and_feature_visualisation.jinja2'

        if eda_source.source_type == models.EdaSourcesEnum.database:
            output_dirs = [
                f'{Path(eda_source.connection_uri).resolve().stem}-{eda_source.tablename}',
                eda_type,
                now.strftime('%Y-%m-%d-%H:%M:%S')
            ]
        # for hdfs and csv files
        else:
            output_dirs = [
                Path(eda_source.connection_uri).resolve().stem,
                eda_type,
                now.strftime('%Y-%m-%d-%H:%M:%S')
            ]


        # folder_to_copy_sum is an intermediate directory
        folder_to_copy_sum = "-".join([
            Path(eda_source.connection_uri).stem,
            now.strftime("%d-%m-%Y-%H-%M-%S")])

        eda_dag_id = "-".join([
            eda_dag_id_prefix,
            Path(eda_source.connection_uri).resolve().stem,
            now.strftime("%d-%m-%Y-%H-%M-%S")])
        code = self.render_template(eda_dag_template,
                                    username=username,
                                    dag_id=eda_dag_id,
                                    source=eda_source,
                                    eda_sources_enum=models.EdaSourcesEnum,
                                    folder_to_copy_sum=folder_to_copy_sum,
                                    now=now)
        with open(os.path.join(settings.DAGS_FOLDER, eda_dag_id + '.py'), 'w') as dag_file:
            dag_file.write(code)

        viz_dag_id = "-".join([
            viz_dag_id_prefix,
            Path(eda_source.connection_uri).resolve().stem,
            now.strftime("%d-%m-%Y-%H-%M-%S")])
        code = self.render_template(viz_dag_template,
                                    username=username,
                                    dag_id=viz_dag_id,
                                    source=eda_source,
                                    output_dirs=output_dirs,
                                    eda_dag_id=eda_dag_id,
                                    folder_to_copy_sum=folder_to_copy_sum,
                                    now=now)
        with open(os.path.join(settings.DAGS_FOLDER, viz_dag_id + '.py'), 'w') as dag_file:
            dag_file.write(code)

        flash_msg = '{} EDA run on Source: {} has been scheduled. '.format(eda_type, str(eda_source)) + \
            'Output will be found in "{}" directory.'.format('/'.join(output_dirs))
        flash(flash_msg)
        return (viz_dag_id, eda_dag_id)

    @expose('/eda/run/<int:source>', methods=['GET', 'POST'])
    @has_access
    @csrf.exempt
    @action_logging
    def run_view(self, source):
        eda_source = models.EdaSource.get_by_id(source_id=source)
        dag_ids = self.create_eda_dags(eda_source, runtype=request.args.get('type', 'L0'))
        # TODO: DO _parse_dags in a separate thread.
        _parse_dags(update_DagModel=True)

        for dag_id in dag_ids:
            unpause_dag(dag_id)
        return redirect(url_for('EDAView.source_view'))

    @expose('/eda/api/', methods=['POST'])
    # @has_access
    @csrf.exempt
    def source_view_api(self, **kwargs):
        # TODO: Add authentication on this view.
        # NOTE: if we create more API's we may want to switch to marshmallow
        # for data valiation
        # print(request.form)
        conn_uri = request.form.get('connection_uri')
        tablename = request.form.get('tablename')
        source_type = request.form.get('source_type')
        source_types = [tipe.value for tipe in models.EdaSourcesEnum]
        if not conn_uri:
            return self.response_400(message="Missing 'connection_uri' in body.")
        if not source_type:
            return self.response_400(message="Missing 'source_type' in body.")
        if source_type not in source_types:
            return self.response_400(message="Invalid 'source_type' in body.")

        self.add_source(conn_uri, source_type, tablename)
        return self.response(201)

    @expose('/eda/<path:source>/delete/', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def source_destroy_view(self, source):
        try:
            self.remove_source(source)
            flash('Source removed')
        except KeyError:
            flash('Source error, {} doesn\'t exists'.format(source))
        return redirect(url_for('EDAView.source_view'))

    @expose('/eda/<path:source>/', methods=['GET'])
    @has_access
    @action_logging
    def list_outputs_view(self, source):
        files = []
        dir_contents = os.listdir(self.output_path)

        for content in dir_contents:
            if content.endswith(('.htm', '.html',)):
                files.append(content)
        return self.render_template('eda/eda_list.html', files=files, source=source)

    @expose('/eda/dashboard/<path:filename>/', methods=['GET'])
    @has_access
    @action_logging
    def dashboard_view(self, filename):
        viz = ''
        try:
            with open(os.path.join(self.output_path, filename)) as f:
                viz = f.read()
        except Exception:
            pass
        return self.render_template('eda/eda_outputs.html', visualisations=viz)

class SparkConfView(AirflowBaseView):

    default_view = 'update_spark_conf'
    def get_files(self, path, ext=[]):
        files = []
        for r, d, f in os.walk(path):
            for file in f:
                print(Path(file).stem, ext)
                if not ext or Path(file).suffix in ext:
                    files.append(file)
        return files

    # remove deleted fields from the conf file
    def delete_fields(self, config):
        try:
            if config.has_option('arguments',
                                 request.form['option_title_args_delete']):
                config.remove_option(
                    'arguments', request.form['option_title_args_delete'])
        except Exception:
            print("Sorry ! No field found in delete in args")

        try:
            if config.has_option('configurations',
                                 request.form['option_title_config_delete']):
                config.remove_option(
                    'configurations',
                    request.form['option_title_config_delete'])
        except Exception:
            print("Sorry ! No field found in delete in config")

    # if the conf file is there initialise args,confids else make the conf
    # file and initialize sections,
    def init_args_configs(self, conf_path, config):
        if os.path.exists(conf_path):
            config.read(filenames=conf_path)
            args = collections.OrderedDict(config.items('arguments'))
            configs = collections.OrderedDict(config.items('configurations'))
        else:
            config.add_section('arguments')
            config.add_section('configurations')
            args = collections.OrderedDict()
            configs = collections.OrderedDict()
        return args, configs

    # update the Arguments, Configurations sections in config file
    def update_args_configs(
            self,
            args,
            configs,
            setup_path,
            keytab_path,
            config):
        for i in args:
            if i == 'jars' or i == 'keytab' or i == 'py-files':
                file_type = {
                    'jars': 'check',
                    'keytab': 'kt_check',
                    'py-files': 'py_check'}
                file_path = {
                    'jars': setup_path,
                    'keytab': keytab_path,
                    'py-files': setup_path}
                files_list = []
                filenames = request.form.getlist(file_type[i])
                for f in filenames:
                    fn = os.path.join(file_path[i], f)
                    files_list.append(fn)
                update_files = ",".join(files_list)
                config.set('arguments', i, update_files)
            else:
                config.set('arguments', i, request.form[i])

        for j in configs:
            config.set('configurations', j, request.form[j])
        return

    # add the new fields to the conf file
    def update_new_args_configs(self, config):
        for key in request.form:
            if request.form[key]:
                key_no = key.split('-')[-1]
                if key.startswith('new-arg-key'):
                    config.set('arguments',
                               request.form[key],
                               request.form['new-arg-value-' + key_no])
                elif key.startswith('new-config-key'):
                    config.set('configurations',
                               request.form[key],
                               request.form['new-config-value-' + key_no])

    @expose('/couture_config/<string:group>', methods=['GET', 'POST'])
    @has_access
    @action_logging
    # function that updates couture_conf.html page and writes changes to
    # couture-spark config file
    def update_spark_conf(self, group):
        title = "Couture Spark Configuration"

        
        config = CP.ConfigParser()
        config.optionxform = str
        conf_path = os.path.join(
            settings.HADOOP_CONFIGS_FOLDER, *[group, 'couture-spark.conf'])
        setup_path = settings.SPARK_DEPENDENCIES_FOLDER 
        keytab_path = os.path.join(
            settings.HADOOP_CONFIGS_FOLDER, *[group, 'keytab'])
        args, configs = self.init_args_configs(conf_path, config)

        files = self.get_files(setup_path, [".jar"])
        print(files, setup_path)
        py_files = self.get_files(setup_path, [".py",".egg",".zip"])
        kt_files = self.get_files(keytab_path)

        if request.method == 'POST':
            config.read(filenames=conf_path)
            args = collections.OrderedDict(config.items('arguments'))
            configs = collections.OrderedDict(config.items('configurations'))
            self.update_args_configs(
                args, configs, setup_path, keytab_path, config)
            self.update_new_args_configs(config)
            self.delete_fields(config)

            # writing all the changes to the config file
            with open(conf_path, 'w') as configfile:
                config.write(configfile)

            args = collections.OrderedDict(config.items('arguments'))
            configs = collections.OrderedDict(config.items('configurations'))

        len_jar = len(files)
        len_py = len(py_files)
        kt_len = len(kt_files)
        return self.render_template(
            'airflow/couture_config.html',
            title=title,
            arguments=args,
            configurations=configs,
            files=files, 
            py_files=py_files,
            len_jar=len_jar, 
            len_py=len_py,
            kt_len=kt_len,
            group=group,
            kt_files=kt_files) 


class LdapConfView(AirflowBaseView):
    default_view = 'update_ldap_conf'

    @expose('/ldap', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_ldap_conf(self):
        import collections
        import configparser as CP
        from airflow.configuration import AIRFLOW_HOME
        config = CP.ConfigParser()
        config.optionxform = str
        conf_path = os.path.normpath(os.path.join(AIRFLOW_HOME, *[os.pardir,
                                                                  'configs',
                                                                  'ldap.conf']))

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


class HelpView(AirflowBaseView):
    default_view = 'help'
    method_permission_name = {
        'help': 'show',
    }

    @expose('/help')
    @has_access
    def help(self):
        try:
            return redirect(url_for('static', filename='Couture_AI_Workflow_Orchestrator.pdf'))
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
    default_view = 'update_keytab'

    @expose('/keytab/<string:group>', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def update_keytab(self, group):
        # NOTE: refactor this method.
        title = "Kerberos Configuration"
        add_to_dir = os.path.join(settings.HADOOP_CONFIGS_FOLDER, *[group, 'keytab'])
        os.makedirs(add_to_dir, exist_ok=True)
        file_name = os.path.join(add_to_dir, 'keytab.conf')
        principal = ''
        keytab_files = ''

        import configparser as CP
        import collections
        config = CP.ConfigParser()
        config.optionxform = str

        if os.path.exists(file_name):
            config.read(filenames=file_name)
        if 'arguments' not in config.sections():
            # Set default values here
            config.add_section('arguments')
            config.set('arguments', 'principal', principal)
            config.set('arguments', 'keytab', keytab_files)
            with open(file_name, 'w') as f:
                config.write(f)
            config.read(filenames=file_name)
        arguments = collections.OrderedDict(config.items('arguments'))
        args = arguments

        if request.method == 'POST':
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

                return redirect(url_for('KeyTabView.update_keytab', group=group))
            except Exception:
                print("Sorry ! No file to delete")

            target = os.path.join(add_to_dir)
            os.makedirs(target, exist_ok=True)

            try:
                for f in request.files.getlist("file"):   # for saving a file
                    filename = f.filename
                    # if filename.endswith(".keytab"):
                    destination = os.path.join(target, filename)
                    f.save(destination)
                    AirflowBaseView.audit_logging("keytab_added", filename, request.environ['REMOTE_ADDR'])
                    flash('File Uploaded!!',
                          "success")
            except Exception:
                print("No file selected!")

            # calling get_details without any extension
            file_data = self.get_details(add_to_dir, ".keytab")
            len_keytab = len(file_data)

            with open(file_name, 'w') as configfile:
                config.write(configfile)

            conf_path = os.path.join(settings.HADOOP_CONFIGS_FOLDER, *[group, 'couture-spark.conf'])
            if os.path.exists(conf_path):
                config.read(filenames=conf_path)
            else:
                config.add_section('arguments')
            config.set('arguments', 'principal', principal)
            config.set('arguments', 'keytab', keytab_files)
            with open(conf_path, 'w') as configfile:
                config.write(configfile)

            return redirect(url_for('KeyTabView.update_keytab', group=group))

        else:
            file_data = self.get_details(add_to_dir, ".keytab")
            len_keytab = len(file_data)
            all_files = []
            for r, d, f in os.walk(add_to_dir):
                for file in f:
                    if file.endswith(".keytab"):
                        all_files.append(file)
            return self.render_template('airflow/keytab.html',
                                        title=title,
                                        group=group,
                                        file_data=file_data,
                                        Arguments=args,
                                        len_keytab=len_keytab,
                                        Files=all_files)

    @expose("/keytab_download/<string:group>/<string:filename>", methods=['GET', 'POST'])
    @has_access
    def download(self, group, filename):  # for downloading the file passed in the filename
        add_to_dir = os.path.join(settings.HADOOP_CONFIGS_FOLDER, *[group])
        keytab_path = os.path.join(add_to_dir, 'keytab')
        path_file = os.path.join(keytab_path, filename)
        AirflowBaseView.audit_logging(
            'KeyTabView.download_keytab',
            f'{group}-{filename}',
            request.environ['REMOTE_ADDR'],
        )
        return send_file(path_file, as_attachment=True, conditional=True)


class JupyterNotebookView(GitIntegrationMixin, AirflowBaseView):
    default_view = 'jupyter_notebook'
    fs_path = settings.JUPYTER_HOME
    class_permission_name = 'Jupyter Notebook'
    method_permission_name = {
        'jupyter_notebook': 'access',
        'jupyter_git_status': 'access',
        'jupyter_git_logs': 'access',
        'push_view': 'access',
        'pull_view': 'access',
        'run_notebook': 'access'
    }

    # add this to global in GitIntegrationMixin
    config_section = 'JupyterNotebook'

    def guess_type(self, val):
        try:
            val = ast.literal_eval(val)
        except ValueError:
            pass
        return val

    def get_kernels(self):
        return [
            'ml-kernel',
            'pysparkkernel',
            'pytorch-kernel',
            'sparkkernel',
            'sparkrkernel',
            'tf-kernel',
            'python3'
        ]

    def create_jupyter_dag(self, notebook, parameters, kernel, schedule=None):
        dag_id = "-".join(["JupyterNotebook",
                           Path(notebook).resolve().stem,
                           datetime.now().strftime("%d-%m-%Y-%H-%M-%S")])
        username = g.user.username
        now = datetime.now()
        code = self.render_template('dags/default_jupyter_dag.jinja2',
                                    notebook=notebook,
                                    username=username,
                                    parameters=parameters,
                                    dag_id=dag_id,
                                    kernel=kernel,
                                    now=now,
                                    schedule=schedule)
        with open(os.path.join(settings.DAGS_FOLDER, dag_id + '.py'), 'w') as dag_file:
            dag_file.write(code)
        # ERROR: (1406, "Data too long for column 'event' at row 1")
        # AirflowBaseView.audit_logging(
        #     'JupyterNoetbook.create_jupyter_dag',
        #     notebook,
        #     request.environ['REMOTE_ADDR'],
        # )
        return dag_id

    @expose('/jupyter_notebook')
    @has_access
    @action_logging
    def jupyter_notebook(self):
        title = "Jupyter Notebook"
        # notebooks = self.get_details(settings.JUPYTER_HOME, '.ipynb')
        current_status, logs = self.get_status()
        return self.render_template('airflow/jupyter_notebook.html',
                                    title=title,
                                    kernels=self.get_kernels(),
                                    # TODO: Load modal in template using ajax
                                    git_template=self.get_git_template(),
                                    view=self.__class__.__name__,
                                    current_status=current_status,
                                    port=settings.JUPYTERHUB_ACCESS_PORT,
                                    logs=logs)

    @expose('/jupyter/status')
    @has_access
    @action_logging
    def jupyter_git_status(self):
        current_status = self.git_status()
        return self.render_template('gitintegration/status_modal.html',
                                    view=self.__class__.__name__,
                                    current_status=current_status)

    @expose('/jupyter/logs')
    @has_access
    @action_logging
    def jupyter_git_logs(self):
        logs = self.git_logs("--pretty='%C(auto)%h %s, Author=<%aN>, Date=%ai'")
        return self.render_template('gitintegration/logs_modal.html',
                                    view=self.__class__.__name__,
                                    logs=logs)

    @expose('/jupyter/commit/', methods=['POST'])
    @has_access
    @action_logging
    def commit_view(self):
        # TODO: Move this to GitIntegrationMixin
        form = request.form
        files_to_commit = []
        commit_file_prefix = 'commit-file-'
        for key, val in form.items():
            if key.startswith(commit_file_prefix) and val:
                files_to_commit.append(key[len(commit_file_prefix):])
        # print(files_to_commit)
        self.git_add(files_to_commit)
        self.git_commit(form['commit-msg'], g.user)
        # TODO: Refine flash message
        flash('Commited files successfully')
        return redirect(url_for('JupyterNotebookView.jupyter_notebook'))

    @expose('/jupyter/push/', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def push_view(self):
        # TODO: Move this to GitIntegrationMixin
        push_status = self.git_push()
        if push_status:
            flash('Pushed successfully!')
        else:
            flash('Unknown error while pushing')
        return redirect(url_for('JupyterNotebookView.jupyter_notebook'))

    @expose('/jupyter/pull/', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def pull_view(self):
        # TODO: Move this to GitIntegrationMixin
        pull_status = self.git_pull()
        if pull_status:
            flash('Pulled successfully!')
        else:
            flash('Unknown error while pulling')
        return redirect(url_for('JupyterNotebookView.jupyter_notebook'))

    @expose('/jupyter/run-notebook/', methods=['POST'])
    @has_access
    @action_logging
    def run_notebook(self):
        notebook = request.form.get('notebook', None)
        kernel = request.form.get('kernel', 'python3')
        if not notebook:
            return make_response(('Missing notebook.', 500))
        abs_notebook_path = os.path.join(self.fs_path, notebook)
        if not os.path.exists(abs_notebook_path):
            flash(f'Notebook {notebook} does not exist.', category='error')
            return redirect(url_for('JupyterNotebookView.jupyter_notebook'))
        parameters = {}
        for key, val in request.form.items():
            if key.startswith('param-key-'):
                key_no = str(key.split('-')[-1])
                parameters[val] = self.guess_type(request.form.get('param-value-' + key_no, ''))
        schedule = request.form.get('schedule')
        if schedule:
            try:
                croniter(schedule)
            except (CroniterBadCronError,
                    CroniterBadDateError,
                    CroniterNotAlphaError):
                flash('Bad Cron Schedule', category='error')
                return redirect(url_for('JupyterNotebookView.jupyter_notebook'))
        else:
            schedule = '@once'
        dag_id = self.create_jupyter_dag(notebook, parameters, kernel=kernel, schedule=schedule)
        flash('Your notebook was scheduled as {}, it should be reflected shortly.'.format(dag_id))

        _parse_dags(update_DagModel=True)
        unpause_dag(dag_id)
        # flash('If dags are paused on default, unpause the created dag.', category='info')
        return redirect(url_for('Airflow.graph', dag_id=dag_id))


class GitConfigView(GitIntegrationMixin, AirflowBaseView):
    default_view = 'git_config_view'
    class_permission_name = 'Git Configuration'
    method_permission_name = {
        'git_config_view': 'access'
    }
    fs_path = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @expose('/git-configs', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def git_config_view(self):
        config = self.read_config()
        if request.method == 'GET':
            return self.render_template('gitintegration/git_config_view.html',
                                        title='Git Configuration View',
                                        sections=self.get_sections(),
                                        keys=self.get_keys(),
                                        config=config)
        form = request.form
        section = form.get('section')
        if not section or section not in self.get_sections():
            return redirect(url_for('GitConfigView.git_config_view'))
        if not config.has_section(section):
            config[section] = {}
        for key in form.keys():
            if key.startswith('config-'):
                cleaned_key = key.split('-')[-1]
                config[section][cleaned_key] = form[key]
        self.write_config(config)
        # TODO: Refine flash message.
        flash('Git config for {}, set succesfully!'.format(section))
        return redirect(url_for('GitConfigView.git_config_view'))


class LivyConfigView(AirflowBaseView):
    default_view = 'livy_config_view'

    base_fs_path = Path(settings.LIVY_CONF_PATH)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Path(self.fs_path).parent.mkdir(exist_ok=True)

    sections = [
        'kernel_python_credentials',
        'kernel_scala_credentials',
        'kernel_r_credentials',
    ]
    keys = {
        'username': {
            'type': 'text',
        },
        'password': {
            'type': 'text'
        },
        'url': {
            'text': 'url',
        },
        'auth': {
            'type': 'select',
            'options': ['None', 'Kerberos', 'Basic_Access']
        },
    }

    def get_sections(self):
        return self.sections

    def get_keys(self):
        return self.keys

    def read_config(self, fs_path):
        try:
            with open(fs_path) as f:
                return json.load(f)
        except (FileNotFoundError, json.decoder.JSONDecodeError,):
            default = {}
            for section in self.get_sections():
                default[section] = {}
            return default

    def write_config(self, config, fs_path):
        # create .sparkmagic dir.
        Path(fs_path).parent.mkdir(exist_ok=True)
        with open(fs_path, 'w') as f:
            json.dump(config, f)

    @expose('/livy-configs/<string:group>', methods=['GET', 'POST'])
    @has_access
    @action_logging
    def livy_config_view(self, group):
        fs_path = os.path.join(self.base_fs_path, *[group, '.sparkmagic', 'config.json'])
        config = self.read_config(fs_path)
        if request.method == 'GET':
            return self.render_template('livyconfig/livy_config_view.html',
                                        title='Livy Configuration View',
                                        config=self.read_config(fs_path),
                                        keys=self.get_keys(),
                                        sections=self.get_sections())
        form = request.form
        section = form.get('section')
        if not section or section not in self.get_sections():
            return redirect(url_for('LivyConfigView.livy_config_view'))

        for key in form.keys():
            if key.startswith('config-'):
                cleaned_key = key.split('-')[-1]
                config[section][cleaned_key] = form[key]
        self.write_config(config, fs_path)
        AirflowBaseView.audit_logging(
            'LivyConfView.livy_config_view',
            '',
            request.environ['REMOTE_ADDR'],
        )
        # TODO: Refine flash message.
        flash('Livy config for Config group {} : {}, set succesfully!'.format(group, section))
        return redirect(url_for('LivyConfigView.livy_config_view', group=group))


class AddDagView(AirflowBaseView):
    default_view = 'add_dag'
    class_permission_name = "Manage DAG"
    method_permission_name = {
        "add_dag": "access",
        "editdag": "access",
        "save_snippet": "access",
        "download": "access",
    }

    # TODO: Refactor this to use FileUploadBaseView.
    # TODO: Refactor Codebricks into its own view.

    # regex for validating filenames while adding new ones
    regex_valid_filenames = re.compile('^[A-Za-z0-9_@()-]+$')
    regex_valid_snippetnames = re.compile('^[\sA-Za-z0-9_@()-]+$') # noqa

    template_dag_file_path = os.path.join(
        app.root_path, *['..', 'config_templates', 'default_dag_template.py'])
    dag_file_template = ''
    try:
        with open(template_dag_file_path, 'r') as f:
            dag_file_template = f.read()
    except Exception:
        pass

    def __init__(self, *args, **kwargs):
        os.makedirs(settings.DAGS_FOLDER, exist_ok=True)
        super().__init__(*args, **kwargs)

    def get_dag_file_path(self, filename):
        # a dag id is sent instead of fileloc.
        if not filename.endswith('.py'):
            dag_orm = DagModel.get_dagmodel(filename)
            return os.path.join(dag_orm.fileloc)
        return os.path.join(settings.DAGS_FOLDER, filename)

    def get_snippet_metadata_path(self):
        return os.path.join(AIRFLOW_HOME, *['repo', 'dag-snippets.json'])

    def get_snippet_file_path(self, title):
        filename = self.snippet_title_to_file(title)
        return os.path.join(AIRFLOW_HOME, *['repo', filename])

    def get_snippets_metadata(self):
        snippets_path = self.get_snippet_metadata_path()
        with open(snippets_path) as f:
            return json.load(f)

    def get_snippets(self):
        snippets_path = self.get_snippet_metadata_path()
        if Path(snippets_path).exists():
            metadata = self.get_snippets_metadata()
            # print(metadata)
            for title in metadata.keys():
                try:
                    with open(self.get_snippet_file_path(title)) as codefile:
                        snippet = codefile.read()
                except Exception:
                    # print(e)
                    snippet = ''
                metadata[title] = {
                    'description': metadata[title],
                    'snippet': snippet
                }
            return metadata
        return dict()

    def snippet_title_to_file(self, title):
        return title.replace(' ', '_') + '.py'

    def save_snippets(self, metadata, new_snippet):
        """Save a new snippet in the repo

        Arguments:
            metadata {dict} -- with keys `title` and `description` of new snippet
            new_snippet {str} -- code of new snippet.
        """
        snippets = self.get_snippets_metadata()

        snippets[metadata['title']] = metadata['description']

        snippets_path = self.get_snippet_metadata_path()
        with open(snippets_path, 'w') as f:
            json.dump(snippets, f)

        with open(self.get_snippet_file_path(metadata['title']), 'w') as f:
            f.write(new_snippet)

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

                        # Thread to update DAGBAG.
                        threading.Thread(target=_parse_dags, args=(True,)).start()

                        flash('File ' + file_name + ' Deleted!!', "warning")
                        break
                # The below redirect makes sure that the ?delete=<filename>
                # is removed from the GET request, as we are redirecting user
                # with 0 GET args. This will prevent any accidental deletion of file.
                return redirect(url_for('AddDagView.add_dag'))
        elif request.method == 'POST':
            list_files = request.files.getlist("file")
            # check if a new filename has been sent to be created.
            filename = request.form.get('filename')
            if len(list_files) > 0:
                files_uploaded = 0
                for upload in request.files.getlist("file"):
                    filename = upload.filename
                    if self.regex_valid_filenames.match(Path(filename).resolve().stem):
                        destination = self.get_dag_file_path(filename)
                        upload.save(destination)
                        AirflowBaseView.audit_logging('dag_added',
                                                      filename,
                                                      request.environ['REMOTE_ADDR'])
                        files_uploaded += 1
                    elif filename:
                        flash('Only python files allowed !, ' + filename + ' not allowed', 'error')

                flash(str(files_uploaded) + ' files uploaded!!', 'success')
                _parse_dags(update_DagModel=True)

            elif filename:
                if self.regex_valid_filenames.match(filename):
                    filename = '.'.join([filename, 'py'])
                    if Path(filename).exists():
                        flash('Dag {} Already present.'.format(filename))
                    else:
                        insert_starter_content = request.form.get('insert-template-content')
                        return redirect(url_for('AddDagView.editdag', filename=filename, insert_starter_content=insert_starter_content, new=True))
                else:
                    flash('Invalid DAG name, DAG not created.', 'error')

            # the below redirect is to avoid form resubmission messages when
            # we refresh the page in the browser.
            return redirect(url_for('AddDagView.add_dag'))
        file_data = self.get_details(dags_dir, ".py")
        return self.render_template('airflow/add_dag.html', title=title, file_data=file_data)

    @expose("/editdag/<string:filename>/", methods=['GET', 'POST'])
    @action_logging
    @has_access
    def editdag(self, filename):
        # NOTE: filename can be the name of a dag file or a dag_id as well.
        fullpath = self.get_dag_file_path(filename)
        new = request.args.get('new', False)
        if not (new or Path(fullpath).exists()) and \
                self.regex_valid_filenames.match(Path(filename).resolve().stem):
            return make_response(('DAG not found', 404))
        if request.method == 'POST':
            code = request.form['code']
            with open(fullpath, 'w') as code_file:
                code_file.write(code)
                flash('Successfully saved !')
                AirflowBaseView.audit_logging(
                    "{}.{}".format(self.__class__.__name__, 'editdag'),
                    filename, request.environ['REMOTE_ADDR'])
                if new:
                    _parse_dags(update_DagModel=True)
                    # TODO Unpause dag here.
                    # unpause_dag(dag_id)
            return redirect(url_for('AddDagView.editdag', filename=filename))
        else:
            if new:
                insert_starter_content = request.args.get('insert_starter_content')
                if(insert_starter_content):
                    code = self.dag_file_template.replace("CoutureExample", os.path.splitext(filename)[0], 1)
                else:
                    code = ""
            else:
                with open(fullpath, 'r') as code_file:
                    code = code_file.read()

        return self.render_template("airflow/editdag.html",
                                    code=code,
                                    filename=filename,
                                    language_server_url=settings.LANGUAGE_SERVER_URL,
                                    dags_folder_path=settings.DAGS_FOLDER,
                                    new=new,
                                    reconnect_interval=settings.RECONNECT_INTERVAL,
                                    snippets=self.get_snippets())

    @expose("/save_snippet/<string:filename>", methods=['POST'])
    @has_access
    @action_logging
    def save_snippet(self, filename):
        snippet_file_path = self.get_snippet_metadata_path()

        # creating a path to tasks_folder (works for python >= 3.5)
        Path(snippet_file_path).parent.mkdir(parents=True, exist_ok=True)

        if request.method == 'POST':
            # snippets = self.get_snippets()

            metadata = {
                'title': request.form['title'],
                'description': request.form['description']
            }
            new_snippet = request.form['snippet']
            self.save_snippets(metadata, new_snippet)
            # with open(snippet_file_path, 'w') as f:
            #     json.dump(snippets, f)

            return redirect(url_for('AddDagView.editdag', filename=filename))
        return make_response(('METHOD_NOT_ALLOWED', 403))

    @expose("/dag_download/<string:filename>", methods=['GET', 'POST'])
    @has_access
    @action_logging
    def download(self, filename):
        path_file = self.get_dag_file_path(filename)
        return send_file(path_file, as_attachment=True)


######################################################################################
#                                    ModelViews
######################################################################################

class DagFilter(BaseFilter):
    def apply(self, query, func): # noqa
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

    formatters_columns = {
        'task_id': wwwutils.task_instance_link,
        'execution_date': wwwutils.datetime_f('execution_date'),
        'timestamp': wwwutils.datetime_f('timestamp'),
        'dag_id': wwwutils.dag_link,
    }

    @action('muldelete', 'Delete', "Are you sure you want to delete selected records?",
            single=False)
    def action_muldelete(self, items):
        self.datamodel.delete_all(items)
        self.update_redirect()
        return redirect(self.get_redirect())

    def pre_add(self, item):
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)

    def pre_update(self, item):
        item.execution_date = timezone.make_aware(item.execution_date)
        item.value = XCom.serialize_value(item.value)


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
        if any(item.pool == models.Pool.DEFAULT_POOL_NAME for item in items):
            flash("default_pool cannot be deleted", 'error')
            self.update_redirect()
            return redirect(self.get_redirect())
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
    edit_template = 'airflow/variable_edit.html'

    datamodel = AirflowModelView.CustomSQLAInterface(models.Variable)

    base_permissions = ['can_add', 'can_list', 'can_edit', 'can_delete', 'can_varimport']

    list_columns = ['key', 'val', 'is_encrypted']
    add_columns = ['key', 'val']
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
        response.headers["Content-Type"] = "application/json; charset=utf-8"
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

    base_permissions = ['can_list', 'can_add']

    add_columns = ['state', 'dag_id', 'execution_date', 'run_id', 'external_trigger', 'conf']
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

    base_permissions = ['can_list', 'can_show']

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

    @has_access
    @permission_name("list")
    @provide_session
    @expose('/autocomplete')
    def autocomplete(self, session=None):
        query = unquote(request.args.get('query', ''))

        if not query:
            wwwutils.json_response([])

        # Provide suggestions of dag_ids and owners
        dag_ids_query = session.query(DagModel.dag_id.label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.dag_id.ilike('%' + query + '%'))

        owners_query = session.query(func.distinct(DagModel.owners).label('item')).filter(
            ~DagModel.is_subdag, DagModel.is_active,
            DagModel.owners.ilike('%' + query + '%'))

        # Hide paused dags
        if request.args.get('showPaused', 'True').lower() == 'false':
            dag_ids_query = dag_ids_query.filter(~DagModel.is_paused)
            owners_query = owners_query.filter(~DagModel.is_paused)

        filter_dag_ids = appbuilder.sm.get_accessible_dag_ids()
        if 'all_dags' not in filter_dag_ids:
            dag_ids_query = dag_ids_query.filter(DagModel.dag_id.in_(filter_dag_ids))
            owners_query = owners_query.filter(DagModel.dag_id.in_(filter_dag_ids))

        payload = [row[0] for row in dag_ids_query.union(owners_query).limit(10).all()]

        return wwwutils.json_response(payload)
