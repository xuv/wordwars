"""Rest API for submitting qarnot jobs in Python."""

__all__ = ["task", "connection", "disk", "notification"]

__version__ = '0.1.0'


class QApyException(Exception):
    """General QApy exception"""
    def __init__(self, msg):
        super(QApyException, self).__init__("Error : {0}".format(msg))

def raise_on_error(response):
    if response.status_code == 503:
        raise QApyException("Service Unavailable")
    if response.status_code != 200:
        raise QApyException(response.json()['message'])

def get_url(key, **kwargs):
    """Get and format the url for the given key.
    """
    urls = {
        'disk folder' : '/disks', #GET -> list; POST -> add
        'disk force' : '/disks/force', # POST -> force add
        'disk info' : '/disks/{name}', # DELETE  -> remove #PUT -> update
        'get disk' : '/disks/archive/{name}.{ext}', #GET-> disk archive
        'tree disk' : '/disks/tree/{name}', #GET -> ls on the disk
        'link disk' : '/disks/link/{name}', #POST -> create links
        'ls disk': '/disks/list/{name}/{path}', #GET -> ls on the dir {path}
        'update file' : '/disks/{name}/{path}', #POST; GET; DELETE
        'list profiles': '/tasks/profiles', #GET -> possible profiles
        'get profile' : '/tasks/profiles/{name}', #GET -> profile info
        'tasks' : '/tasks', #GET -> runing tasks; POST -> submit task
        'task force' : '/tasks/force', #POST -> force add
        'task update' : '/tasks/{uuid}', #GET->result, DELETE->abort, #PATCH->update resources
        'task snapshot': '/tasks/{uuid}/snapshot/periodic', #POST -> snapshots
        'task instant' : '/tasks/{uuid}/snapshot', #POST-> get a snapshot
        'task stdout': '/tasks/{uuid}/stdout', #GET -> task stdout
        'task stderr': '/tasks/{uuid}/stderr', #GET -> task stderr
        'user': '/info', #GET -> user info
        'notification' : '/notifications', #GET -> notifications list #POST -> add notification
        'notification update' : '/notifications/{uuid}' #GET -> notification info, #DELETE -> remove notification #PUT -> update
    }
    return urls[key].format(**kwargs)

import qapy.connection
QApy = qapy.connection.QApy
