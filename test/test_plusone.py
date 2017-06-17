import unittest
from pydefm import plusone

class PrimesTestCase(unittest.TestCase):
    """Tests for `primes.py`."""

    def test_is_five_prime(self):
        """Is five successfully determined to be prime?"""
        self.assertTrue(plusone.is_prime(5))

if __name__ == '__main__':
    unittest.main()
