"""Notification"""

from qapy import get_url, raise_on_error

class QNotification(object):
    """A Qarnot Notification
    """
    def __init__(self, jsonnotification, connection):
        """Initialize a notification from a dictionnary

        :param dict jsonnotification: Dictionnary representing the notification,
                must contain following keys:

                  * id: string, the notification's GUID
                  * mask: TaskStateChanged
                  * filter.destination: string, destination (email)
                  * filter.filterKey
                  * filter.filterValue

                optionnal
                  * filter.template Mail template for the notification
                  * filter.to To state regex (default to .*)
                  * filter.from From state regex (default to .*)
                  * filter.state From or To state regex (default to .*)


        """
        self._connection = connection

        self._id = jsonnotification['id']
        self._mask = jsonnotification['mask']

        destination = jsonnotification['filter']['destination']
        template = jsonnotification['filter']['template'] if 'template' in jsonnotification['filter'] else None

        filterkey = jsonnotification['filter']['filterKey']
        filtervalue = jsonnotification['filter']['filterValue']

        if self._mask == "TaskStateChanged":
            _from = jsonnotification['filter']['from']
            state = jsonnotification['filter']['state']
            to = jsonnotification['filter']['to']
            self._filter = TaskStateChanged(template, destination, filterkey, filtervalue, to, _from, state)
        elif self._mask == "TaskCreated":
            self._filter = TaskCreated(template, destination, filterkey, filtervalue)
        elif self._mask == "TaskEnded":
            self._filter = TaskEnded(template, destination, filterkey, filtervalue)

    @classmethod
    def _create(cls, connection, _filter):
        """Create a new QNotification
        """
        data = {
            "mask" : type(_filter).__name__,
            "filter" : _filter.json()
            }
        url = get_url('notification')
        response = connection._post(url, json=data)
        raise_on_error(response)
        rid = response.json()['guid']
        response = connection._get(get_url('notification update', uuid=rid))
        raise_on_error(response)
        return QNotification(response.json(), connection)

    def delete(self):
        """Delete the notification represented by this :class:`QNotification`.

        :raises qapy.QApyException: API general error, see message for details
        :raises qapy.connection.UnauthorizedException: invalid credentials
        """

        response = self._connection._delete(
            get_url('notification update', uuid=self._id))
        raise_on_error(response)

    @property
    def id(self):
        """Id Getter
        """
        return self._id

    @property
    def filter(self):
        """Filter getter
        """
        return self._filter

    @filter.setter
    def filter(self, value):
        """Filter setter
        """
        self._filter = value

class Filter(object):
    """Filter class
    """
    def __init__(self, template, destination):
        self._template = template
        self._destination = destination

    def json(self):
        """Json representation of the class
        """
        json = {}
        json["destination"] = self._destination
        if self._template is not None:
            json["template"] = self._template
        return json

    @property
    def destination(self):
        """Destination getter
        """
        return self._destination

    @destination.setter
    def destination(self, value):
        """Destination setter
        """
        self._destination = value

    @property
    def template(self):
        """Template getter
        """
        return self._template

    @template.setter
    def template(self, value):
        """Template setter
        """
        self._template = value

class TaskNotification(Filter):
    """TaskNotification class
    """
    def __init__(self, template, destination, filterkey, filtervalue):
        Filter.__init__(self, template, destination)
        self._filterkey = filterkey
        self._filtervalue = filtervalue

    def json(self):
        json = Filter.json(self)
        json["filterKey"] = self._filterkey
        json["filterValue"] = self._filtervalue
        return json

    @property
    def filterkey(self):
        """Filterkey getter
        """
        return self._filterkey

    @filterkey.setter
    def filterkey(self, value):
        """Filterkey setter
        """
        self._filterkey = value

    @property
    def filtervalue(self):
        """Filtervalue getter
        """
        return self._filtervalue

    @filtervalue.setter
    def filtervalue(self, value):
        """Filtervalue setter
        """
        self._filtervalue = value

class TaskStateChanged(TaskNotification):
    """TaskStateChanged class
    """
    def __init__(self, template, destination, filterkey, filtervalue, to, _from, state):
        TaskNotification.__init__(self, template, destination, filterkey, filtervalue)
        self._to = to
        self._from = _from
        self._state = state

    def json(self):
        json = TaskNotification.json(self)
        if self._to is not None:
            json["to"] = self._to
        if self._from is not None:
            json["from"] = self._from
        if self._state is not None:
            json["state"] = self._state
        return json

    @property
    def toregex(self):
        """To getter
        """
        return self._to

    @toregex.setter
    def toregex(self, value):
        """To setter
        """
        self._to = value

    @property
    def fromregex(self):
        """To getter
        """
        return self._from

    @fromregex.setter
    def fromregex(self, value):
        """To setter
        """
        self._from = value

    @property
    def stateregex(self):
        """To getter
        """
        return self._state

    @stateregex.setter
    def state_regex(self, value):
        """To setter
        """
        self._state = value

class TaskCreated(TaskNotification):
    """TaskCreated class
    """
    def __init__(self, template, destination, filterkey, filtervalue):
        TaskNotification.__init__(self, template, destination, filterkey, filtervalue)

    def json(self):
        json = TaskNotification.json(self)
        return json

class TaskEnded(TaskNotification):
    """TaskEnded class
    """
    def __init__(self, template, destination, filterkey, filtervalue):
        TaskNotification.__init__(self, template, destination, filterkey, filtervalue)

    def json(self):
        json = TaskNotification.json(self)
        return json
