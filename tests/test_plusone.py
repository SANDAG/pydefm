import pytest
from pandas.util.testing import assert_frame_equal


from pydefm import plusone


def test_answer():
    assert plusone.inc(3) == 4


