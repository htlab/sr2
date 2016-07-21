# -*- coding: utf-8 -*-


from fabric.contrib.project import rsync_project
from fabric.api import task, sudo, lcd, cd, env, run, local, put


from fabtools.files import is_dir, is_file
import fabtools.user
from fabtools import require
import fabtools.vagrant

app_name = 'sr2'
env.hosts = ['sox-recorder.ht.sfc.keio.ac.jp']


def root_rsync(local_dir, remote_dir, exclude=[], delete=False):
    import hashlib

    def _end_with_slash(dir_path):
        if dir_path[-1] == '/':
            return dir_path
        else:
            return dir_path + '/'

    local_dir = _end_with_slash(local_dir)
    remote_dir = _end_with_slash(remote_dir)
    m = hashlib.md5()
    m.update(remote_dir)
    me = local('whoami', capture=True)
    remote_tmp_dir = '/tmp/%s/%s/' % (me, m.hexdigest())
    run('mkdir -p %s' % remote_tmp_dir)
    if is_dir(remote_dir):
        run('rsync -a %s %s' % (remote_dir, remote_tmp_dir))  # already exists
    rsync_project(
        remote_dir=remote_tmp_dir,
        local_dir=local_dir,
        exclude=exclude,
        delete=delete
    )
    sudo('rsync -a %s %s' % (remote_tmp_dir, remote_dir))


@task
def vagrant():
    fabtools.vagrant.vagrant()


@task
def setup_python():
    ez_setup_url = 'https://bootstrap.pypa.io/ez_setup.py'
    sudo('curl %s -o /tmp/ez_setup.py' % ez_setup_url)
    sudo('python /tmp/ez_setup.py')
    sudo('easy_install pip')
    sudo('pip install virtualenv')


@task
def tunnel():
    # ssh -L 10022:sox-recorder.ht.sfc.keio.ac.jp:22 dali
    env.hosts = ['localhost:10022']


@task
def setup():
    sudo('apt-get install -y python-dev autoconf g++')

    # install java
    # sudo('apt-get install -y openjdk-7-jre-headless')
    _setup_java()

    # install nginx
    sudo('apt-get install -y nginx')

    # TODO: put nginx config

    require.files.directory(
        '/usr/local/%s' % app_name,
        owner='root', group='root', use_sudo=True
    )

    recorder_logfile = '/var/log/sr2-recorder.log'
    if not is_file(recorder_logfile):
        require.files.file(
            recorder_logfile,
            contents='',
            owner='root', group='root', use_sudo=True
        )

    controller_logfile = '/var/log/sr2-controller.log'
    if not is_file(controller_logfile):
        require.files.file(
            controller_logfile,
            contents='',
            owner='root', group='root', use_sudo=True
        )

    _setup_postgresql()

    deploy()


def _setup_java():
    sudo('echo debconf shared/accepted-oracle-license-v1-1 select true | sudo debconf-set-selections')
    sudo('echo debconf shared/accepted-oracle-license-v1-1 seen true | sudo debconf-set-selections')
    sudo('add-apt-repository -y ppa:webupd8team/java')
    sudo('apt-get update')
    sudo('apt-get install -y oracle-java8-installer')


@task
def setup_postgresql():
    _setup_postgresql()


def _setup_postgresql():
    # install postgresql
    sudo('apt-get install -y postgresql-9.3')

    # fabtools.user.modify('postgres', password='postgresmiro')

    # put pg_hba.conf to change "peer" auth policy to "md5"
    require.files.file(
        '/etc/postgresql/9.3/main/pg_hba.conf',
        source='./files/postgresql/etc/postgresql/9.3/main/pg_hba.conf',
        owner='postgres', group='postgres', use_sudo=True, mode='0600'
    )

    # TODO: psql に pg user つくる
    require.postgres.user('sr2', password='sr2miro')

    # TODO: psql に pg db つくる
    require.postgres.database('sr2', owner='sr2')

    # TODO: psql にschemaながしこむ
    schema_flag_file = '/usr/local/%s/schema.flag' % app_name
    if not is_file(schema_flag_file):
        schema_file = './schema.sql'
        require.files.file(
            '/tmp/schema.sql',
            source=schema_file,
            owner='root', group='root', use_sudo=True
        )
        run('psql -U sr2 -d sr2 -f /tmp/schema.sql')
        sudo('rm -f /tmp/schema.sql')
        require.files.file(
            schema_flag_file,
            contents='created',
            owner='root', group='root', use_sudo=True
        )


@task
def setup_supervisor():
    # NOTE: DID NOT CHECK IF IT WORKS !!!
    # NOTE: for debian-ish linux
    sudo('pip install supervisor')
    sudo('mkdir -p /etc/supervisor/conf.d')
    sudo('mkdir -p /var/log/supervisor')

    require.files.file(
        '/etc/init.d/supervisor',
        source='./files/supervisor/etc/init.d/supervisor',
        owner='root', group='root', mode='0755', use_sudo=True
    )

    require.files.file(
        '/etc/supervisor/supervisord.conf',
        source='./files/supervisor/etc/supervisor/supervisord.conf',
        owner='root', group='root', use_sudo=True
    )

    sudo('/etc/init.d/supervisor start')


@task
def deploy():
    _deploy_common()
    _deploy_java()
    _deploy_python()


def _deploy_common():
    require.files.directory(
        '/usr/local/%s/app' % app_name,
        owner='root', group='root', use_sudo=True
    )

    root_rsync(
        './',
        '/usr/local/%s/app/' % app_name,
        exclude=['.git', 'data', '.venv', '*.tmp', '.DS_Store', '*.pyc', '*.egg-info'],
        delete=True
    )


@task
def deploy_java():
    _deploy_common()
    _deploy_java()


@task
def deploy_python():
    _deploy_common()
    _deploy_python()


def _deploy_java():
    # TODO: deploy java program
    with lcd('./java'):
        local('gradle buildRecorder')

    javabin_dir = '/usr/local/%s/javabin' % app_name
    if not is_dir(javabin_dir):
        require.files.directory(
            javabin_dir,
            owner='root', group='root', use_sudo=True
        )

    require.files.file(
        '%s/recorder.jar' % javabin_dir,
        source='./java/build/libs/recorder-0.0.1.jar',
        owner='root', group='root', use_sudo=True
    )

    require.files.file(
        '/usr/local/%s/recorder.secret.ini' % app_name,
        source='./files/sr2/recorder.secret.ini',
        owner='root', group='root', use_sudo=True, mode='0700'
    )

    require.files.file(
        '/etc/supervisor/conf.d/sr2-recorder.conf',
        source='./files/supervisor/etc/supervisor/conf.d/sr2-recorder.conf',
        owner='root', group='root', use_sudo=True
    )

    sudo('supervisorctl reload')
    sudo('supervisorctl restart sr2-recorder')


def _deploy_python():
    # TODO: deploy python program

    # TODO: pip install -r requirements.txt

    # TODO: python setup.py develop
    pass
