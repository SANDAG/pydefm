import pytest
from pydefm import plusone


def test_answer():
    assert plusone.inc(3) == 4
