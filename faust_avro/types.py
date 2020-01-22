from datetime import datetime, time


class int32(int):
    """An integer subclass which can be used as a type in python to force
    an avro int type instead of an avro long."""

    pass


class float32(float):
    """A float subclass which can be used as a type in python to force
    an avro float type instead of an avro double."""

    pass


class time_millis(time):
    """A time subclass which can be used as a type in python to force
    an avro logical type of time-millis."""

    pass


class datetime_millis(datetime):
    """A datetime subclass which can be used as a type in python to force
    an avro logical type of timestamp-millis."""

    pass
