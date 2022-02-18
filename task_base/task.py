import os
from datetime import datetime, timezone


class Task(object):
    DATE_FORMAT = "%Y-%m-%d"
    ASSET_TIMESTAMP_PROPERTY = "system:time_start"

    # possible statuses associated with a Task instance -- which is different from ee task statuses
    NOTSTARTED = "not started"
    FAILED = "failed"
    RUNNING = "running"
    COMPLETE = "complete"
    status = NOTSTARTED
    inputs = {}

    def _set_inputs(self, prop):
        if not hasattr(self, prop):
            return
        inputs = getattr(self, prop)
        for input_key, i in inputs.items():
            for key, val in i.items():
                if not isinstance(val, str) or not hasattr(self.__class__, val):
                    continue
                func = getattr(self.__class__, val)
                if callable(func):
                    inputs[input_key][key] = func(self)

    def __init__(self, *args, **kwargs):
        _taskdate = datetime.now(timezone.utc).date()
        _taskdatestr = kwargs.get("taskdate") or os.environ.get("taskdate")
        try:
            _taskdate = datetime.strptime(_taskdatestr, self.DATE_FORMAT).date()
        except (TypeError, ValueError):
            pass
        self.taskdate = _taskdate

        self.overwrite = kwargs.get("overwrite") or os.environ.get("overwrite") or False
        self.raiseonfail = (
            kwargs.get("raiseonfail") or os.environ.get("raiseonfail") or True
        )

        self._set_inputs("common_inputs")
        self._set_inputs("inputs")

    def check_inputs(self):
        pass

    def calc(self):
        raise NotImplementedError("`calc` must be defined")

    def wait(self):
        pass

    def clean_up(self, **kwargs):
        pass

    def run(self, **kwargs):
        try:
            self.status = self.RUNNING
            self.check_inputs()
            if self.status != self.FAILED:
                try:
                    self.calc()
                    self.wait()
                    self.status = self.COMPLETE
                except Exception as e:
                    self.status = self.FAILED
                    if self.raiseonfail:
                        raise e
        finally:
            self.clean_up()
        print("status: {}".format(self.status))
