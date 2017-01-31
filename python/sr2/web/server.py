# -*- coding: utf-8 -*-
import sys
import os
import os.path
import json
import traceback

import click
import bottle
# from jinja2 import Template
from jinja2 import Environment, FileSystemLoader
from jinja2.utils import select_autoescape
from bottle import Bottle, HTTPResponse, request, static_file, redirect
from bottleapi.jsonapi import json_endpoint
from bottleapi import WebApiError
from beaker.middleware import SessionMiddleware

from sr2 import models
from sr2.models import (
    USER_LEVEL_ADMIN,
    USER_LEVEL_NORMAL,
    ApiKey,
    User
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
        self._template_dir = template_dir
        self._static_dir = static_dir
        self._template_cache = template_cache

        self._templates = {}
        self._tpl_loader = Environment(
            loader=FileSystemLoader(self._template_dir, encoding='utf-8'),
            autoescape=select_autoescape(
                enabled_extensions=('html',),
                default_for_string=True,
                default=True
            )
        )

        models.connect(
            pg_info.get('host', 'localhost'),
            pg_info.get('port', 5432),
            pg_info.get('user', 'sr2'),
            pg_info.get('pw', 'sr2miro'),
            pg_info.get('db', 'sr2')
        )

    def _render(self, template_name, *args, **kwargs):
        if template_name not in self._templates:
            # tpl_path = os.path.join(self._template_dir, template_name)
            # with open(tpl_path, 'rb') as fh:
            #     print '@@@ open file ok'
            #     template = Template(fh.read().decode('utf-8'))
            template = self._tpl_loader.get_template(template_name)

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
    def api_record_latest(self):
        pass

    @json_endpoint
    @log_exc
    def api_record_latest_value(self):
        pass

    @log_exc
    def record_latest_value(self):
        pass

    @log_exc
    def static(self, static_file_path):
        return static_file(static_file_path, root=self._static_dir)

    def html_index(self):
        user = self._session_auth()
        if not user:
            return redirect('/login')
        return self._render('index.html.j2', user=user)

    def html_login(self):
        err_msg = None
        if request.method == 'POST':
            login = request.params.get('login', '')
            pw = request.params.get('pw', '')
            if self._auth(login, pw):
                session = request.environ.get('beaker.session')
                session['login'] = login
                session['pw'] = pw
                return redirect('/')
            else:
                err_msg = 'Authentication ERROR'
        return self._render('login.html.j2', err_msg=err_msg)

    def logout(self):
        session = request.environ.get('beaker.session')
        session['login'] = ''
        session['pw'] = ''
        return redirect('/login')

    def html_admin_users(self):
        user = self._session_auth_admin()
        if not user:
            return redirect('/login')

        users = User.select()
        return self._render('admin/users.html.j2', user=user, users=users)

    def html_admin_users_new(self):
        user = self._session_auth_admin()
        if not user:
            return redirect('/login')

        err_msg = []
        input_login = ''

        if request.method == 'POST':
            login = request.params.get('login', None)
            if not login:
                err_msg.append('"login" required')
            input_login = login

            pw = request.params.get('pw', None)
            if not pw:
                err_msg.append('"password" required')

            pw2 = request.params.get('pw2', None)
            if pw != pw2:
                err_msg.append('"password(confirm)" does not match to password')

            level = request.params.get('level', str(USER_LEVEL_NORMAL))
            try:
                level = int(level)
            except:
                err_msg.append('malformed input: "level"')
            else:
                if level not in (USER_LEVEL_NORMAL, USER_LEVEL_ADMIN):
                    err_msg.append('bad value for "level"')

            if not err_msg:
                new_user = User()
                new_user.login = login
                new_user.assign_password(pw)
                new_user.level = level
                new_user.save()
                return redirect('/admin/users/')

        return self._render(
            'admin/users_new.html.j2',
            user=user,
            err_msg=err_msg,
            input_login=input_login
        )

    def html_admin_users_delete(self, del_user_id):
        user = self._session_auth_admin()
        if not user:
            return redirect('/login')

        del_user = User.select().where(User.id == int(del_user_id)).first()
        if not del_user:
            return redirect('/admin/users/')

        if request.method == 'POST':
            confirm = request.params.get('confirm', '')
            if confirm != 'yes':
                return self._render(
                    'admin/users_delete.html.j2',
                    user=user,
                    del_user=del_user,
                    err_msg='please confirm'
                )

            del_user.delete_instance()
            return redirect('/admin/users/')

        return self._render(
            'admin/users_delete.html.j2',
            user=user,
            del_user=del_user,
            err_msg=None
        )

    def html_admin_users_pwreset(self, pw_user_id):
        user = self._session_auth_admin()
        if not user:
            return redirect('/login')

        pw_user = User.select().where(User.id == int(pw_user_id)).first()
        if not pw_user:
            return redirect('/admin/users/')

        err_msg = None

        if request.method == 'POST':
            pw = request.params.get('pw', None)
            if not pw:
                err_msg = '"new password" required'

            pw2 = request.params.get('pw2', None)
            if pw != pw2:
                err_msg = '"new password(confirm)" does not match'

            if not err_msg:
                pw_user.assign_password(pw)
                pw_user.save()

                if pw_user.id == user.id:
                    session = request.environ.get('beaker.session')
                    session['login'] = user.login
                    session['pw'] = pw

                return redirect('/admin/users/')
        return self._render(
            'admin/users_pwreset.html.j2',
            user=user,
            pw_user=pw_user,
            err_msg=err_msg
        )

    def html_apikeys(self):
        user = self._session_auth()
        if not user:
            return redirect('/login')
        api_keys = list(user.api_keys)
        return self._render('apikeys/index.html.j2', user=user, api_keys=api_keys)

    def html_apikeys_new(self):
        user = self._session_auth()
        if not user:
            return redirect('/login')

        if request.method != 'POST':
            print '@@@ not POST'
            return redirect('/apikeys/')

        new_key = ApiKey()
        new_key.user = user
        new_key.user_id = user.id
        new_key.is_enabled = True
        new_key.assign_random_key()
        new_key.save(force_insert=True)
        print '@@@ saved'
        return redirect('/apikeys/')

    def html_apikeys_delete(self, del_key_id):
        pass  # TODO

    def html_apikeys_update(self):
        user = self._session_auth()
        if not user:
            return redirect('/login')

        if request.method == 'POST':
            # apikey_ids = request.params.get('apikey_ids', '')
            ids = request.params.getlist('apikey_ids')
            op = request.params.get('op', '')
            print 'ids=%s' % ids
            print 'op=%s' % op
            if op == 'Enable':
                print '@@@ enable'
                ApiKey.update(is_enabled=True).where((ApiKey.user_id == user.id) & (ApiKey.api_key << ids)).execute()
            elif op == 'Disable':
                print '@@@ disable'
                ApiKey.update(is_enabled=False).where((ApiKey.user_id == user.id) & (ApiKey.api_key << ids)).execute()
            else:
                print '@@@ unknown'

        return redirect('/apikeys/')

    def _auth_or_api_key(self):
        # requires login or api_key param
        session = request.environ.get('beaker.session')
        session_login = session.get('login', None)
        session_pw = session.get('pw', None)

        auth_user = self._auth(session_login, session_pw)
        if auth_user:
            return True

        api_key = request.params.get('api_key', None)
        if self._is_valid_apikey(api_key):
            return True

        return False

    def _session_auth(self):
        session = request.environ.get('beaker.session')
        session_login = session.get('login', None)
        session_pw = session.get('pw', None)
        return self._auth(session_login, session_pw)

    def _session_auth_admin(self):
        auth_user = self._session_auth()
        if not auth_user:
            return None
        return auth_user if auth_user.is_admin() else None

    def _auth(self, login, pw):
        user = User.select().where(User.login == login).first()
        if not user:
            print 'bad user'
            return None
        # return user if user.auth(pw) else None
        if user.auth(pw):
            print 'pw is correct!'
            return user
        else:
            print 'pw is incorrect...'
            return None

    def _is_valid_apikey(self, api_key):
        db_key = ApiKey.get(ApiKey.api_key == api_key).first()
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

        app.route('/api/record/latest', ['GET'], self.api_record_latest)
        app.route('/api/record/latest/value', ['GET'], self.api_record_latest_value)

        app.route('/', ['GET'], self.html_index)
        app.route('/admin/users/', ['GET'], self.html_admin_users)
        app.route('/admin/users/new', ['POST'], self.html_admin_users_new)
        app.route('/admin/users/delete/<del_user_id>', ['GET', 'POST'], self.html_admin_users_delete)
        app.route('/admin/users/pwreset/<pw_user_id>', ['GET', 'POST'], self.html_admin_users_pwreset)

        app.route('/apikeys/', ['GET'], self.html_apikeys)
        app.route('/apikeys/new', ['GET', 'POST'], self.html_apikeys_new)
        app.route('/apikeys/update', ['POST'], self.html_apikeys_update)
        app.route('/apikeys/delete/<del_key_id>', ['GET', 'POST'], self.html_apikeys_delete)

        app.route('/login', ['GET', 'POST'], self.html_login)
        app.route('/logout', ['GET', 'POST'], self.logout)

        # wrap with beaker
        app = SessionMiddleware(app, self._get_session_opts())

        return app


@click.command()
@click.option('-c', '--config-file', help='configuration file(json)')
def main(config_file):
    if not config_file:
        print 'missing parameter: -c/--config-file'
        sys.exit(1)
    elif not os.path.exists(config_file):
        print 'missing config file: %s' % config_file
        sys.exit(1)

    with open(config_file, 'rb') as fh:
        config = json.load(fh)

    bind_addr = config.get('bind_addr', '0.0.0.0')
    port = config.get('port', 43280)

    app = SR2Controller.build_from_config(config_file)
    wsgi_app = app.create_wsgi_app()

    from gevent.pywsgi import WSGIServer

    server = WSGIServer((bind_addr, port), wsgi_app)
    print 'port=%d' % port
    server.serve_forever()


if __name__ == '__main__':
    main()
