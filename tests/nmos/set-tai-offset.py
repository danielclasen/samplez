"""Sets the kernel's TAI offset (adjtimex ADJ_TAI), so CLOCK_TAI reads TAI.

usage: python3 set-tai-offset.py <seconds>    (needs CAP_SYS_TIME)
"""

import ctypes
import sys
import time

ADJ_TAI = 0x0080
c_long, c_int = ctypes.c_long, ctypes.c_int


class Timeval(ctypes.Structure):
    _fields_ = [("tv_sec", c_long), ("tv_usec", c_long)]


# struct timex from <sys/timex.h> on Linux.
class Timex(ctypes.Structure):
    _fields_ = [
        ("modes", ctypes.c_uint), ("offset", c_long), ("freq", c_long), ("maxerror", c_long),
        ("esterror", c_long), ("status", c_int), ("constant", c_long), ("precision", c_long),
        ("tolerance", c_long), ("time", Timeval), ("tick", c_long), ("ppsfreq", c_long),
        ("jitter", c_long), ("shift", c_int), ("stabil", c_long), ("jitcnt", c_long),
        ("calcnt", c_long), ("errcnt", c_long), ("stbcnt", c_long), ("tai", c_int),
        ("padding", c_int * 11),
    ]


def main(seconds):
    tx = Timex(modes=ADJ_TAI, constant=int(seconds))
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.adjtimex(ctypes.byref(tx)) < 0:
        raise OSError(ctypes.get_errno(), "adjtimex")
    offset = time.clock_gettime(time.CLOCK_TAI) - time.clock_gettime(time.CLOCK_REALTIME)
    print(f"CLOCK_TAI - CLOCK_REALTIME = {offset:.3f} s")
    return 0 if round(offset) == int(seconds) else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
