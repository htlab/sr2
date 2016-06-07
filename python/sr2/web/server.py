# -*- coding: utf-8 -*-
import sys
import os
import os.path
import json
import traceback

import click
import bottle
from jinja2 import Template
from bottle import Bottle, HTTPResponse, request, static_file
from bottleapi.jsonapi import json_endpoint
from bottleapi import WebApiError
from beaker.middleware import SessionMiddleware

from sr2 import models
from sr2.models import (
    ApiKey
)


def log_exc(func):
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except WebApiError:
            raise
        except HTTPResponse:
            raise
        except:
            traceback.print_exc()  # TODO: use logging.exception instead
            raise
    _func.__name__ = func.__name__
    return _func


class SR2Controller(object):

    @staticmethod
    def build_from_config(config_file):
        with open(config_file, 'rb') as fh:
            config = json.load(fh)

        here = os.path.abspath(os.path.dirname(__file__))
        template_dir = config.get('template_dir', None) or os.path.join(here, 'template')
        static_dir = config.get('static_dir', None) or os.path.join(here, 'static')
        pg_info = config.get('postgresql', {})
        template_cache = config.get('template_cache', False)

        return SR2Controller(pg_info, template_dir, static_dir, template_cache)

    def __init__(self, pg_info, template_dir, static_dir, template_cache):
        self._pg_info = pg_info
        self._template_dir = templat_dir
        self._static_dir = static_dir
        self._template_cache = template_cache

        self._templates = {}

        models.connect(
            pg_info.get('host', 'localhost'),
            pg_info.get('port', 5432),
            pg_info.get('user', 'sr2'),
            pg_info.get('pw', 'sr2miro'),
            pg_info.get('db', 'sr2')
        )

    def _render(self, template_name, *args, **kwargs):
        if template_name not in self._templates:
            tpl_path = os.path.join(self._templat_dir, template_name)
            with open(tpl_path, 'rb') as fh:
                template = Template(fh.read().decode('utf-8'))
                if self._template_cache:
                    self._templates[template_name] = template
        else:
            template = self._templates[template_name]
        return template.render(**kwargs)

    @json_endpoint
    @log_exc
    def api_keytest(self):
        api_key = request.params.get('api_key', None)
        is_existing_key = self._is_valid_apikey(api_key)
        return dict(api_key=api_key, valid=is_existing_key)

    @json_endpoint
    @log_exc
    def api_observation_add(self):
        pass

    @json_endpoint
    @log_exc
    def api_observation_start(self):
        pass

    @json_endpoint
    @log_exc
    def api_observation_stop(self):
        pass

    @json_endpoint
    @log_exc
    def api_server_list(self):
        pass

    @json_endpoint
    @log_exc
    def api_export_start(self):
        pass

    @json_endpoint
    @log_exc
    def api_export_list(self):
        pass

    @json_endpoint
    @log_exc
    def api_export_status(self):
        pass

    @json_endpoint
    @log_exc
    def api_export_update_state(self):
        pass

    @json_endpoint
    @log_exc
    def api_record_latest(self):
        pass

    @json_endpoint
    @log_exc
    def api_record_latest_value(self):
        pass

    @log_exc
    def export_download(self, export_id):
        pass

    @log_exc
    def record_latest_value(self):
        pass

    @log_exc
    def static(self, static_file_path):
        return static_file(static_file_path, root=self._static_dir)

    def html_index(self):
        pass  # TODO

    def html_export_new(self):
        pass  # TODO

    def html_export_list(self):
        pass  # TODO

    def _auth_or_api_key(self):
        # requires login or api_key param
        session = request.environ.get('beaker.session')
        session_login = session.get('login', None)
        session_pw = session.get('pw', None)

        session_auth = self._auth(session_login, session_pw)
        if session_auth:
            return True

        api_key = request.params.get('api_key', None)
        if self._is_valid_apikey(api_key):
            return True

        return False

    def _auth(self, login, hash_pw):
        user = User.get(User.login == login)
        if not user:
            return False

        return (user.hash_pw == hash_pw)

    def _is_valid_apikey(self, api_key):
        db_key = ApiKey.get(ApiKey.api_key == api_key)
        return (db_key is not None)

    def _get_session_opts(self):
        session_opts = {  # FIXME
            'session.type': 'file',
            'session.cookie_expires': 86400 * 5,
            'session.data_dir': '/tmp/sr2-controller.session',
            'session.auto': True
        }
        return session_opts

    def create_wsgi_app(self):
        app = Bottle()

        app.route('/static/<static_file_path:path>', ['GET'], self.static)

        app.route('/api/keytest', ['GET', 'POST'], self.api_keytest)

        app.route('/api/observation/add', ['GET', 'POST'], self.api_observation_add)
        app.route('/api/observation/start', ['GET', 'POST'], self.api_observation_start)
        app.route('/api/observation/stop', ['GET', 'POST'], self.api_observation_stop)

        app.route('/api/server/list', ['GET', 'POST'], self.api_server_list)

        app.route('/api/export/start', ['GET', 'POST'], self.api_export_start)
        app.route('/api/export/list', ['GET', 'POST'], self.api_export_list)
        app.route('/api/export/status', ['GET', 'POST'], self.api_export_status)
        app.route('/api/export/update-state', ['GET', 'POST'], self.api_export_update_state)
        app.route('/api/record/latest', ['GET'], self.api_record_latest)
        app.route('/api/record/latest/value', ['GET'], self.api_record_latest_value)

        app.route('/export/download/<export_id>', ['GET'], self.export_download)

        app.route('/', ['GET'], self.html_index)
        app.route('/export/new', ['GET'], self.html_export_new)
        app.route('/export/list', ['GET'], self.html_export_list)

        # wrap with beaker
        app = SessionMiddleware(app, self._get_session_opts())

        return app


@click.command()
@click.option('-c', '--config-file', help='configuration file')
def main(config_file):
    if not os.path.exists(config_file):
        print 'missing config file: %s' % config_file
        sys.exit(-1)

    with open(config_file, 'rb') as fh:
        config = json.load(fh)

    bind_addr = config.get('bind_addr', '0.0.0.0')
    port = config.get('port', 43280)

    app = SR2Controller.build_from_config(config_file)
    wsgi_app = app.create_wsgi_app()

    from gevent.pywsgi import WSGIServer

    server = WSGIServer((bind_addr, port), wsgi_app)
    server.serve_forever()


if __name__ == '__main__':
    main()
