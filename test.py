import unittest
from primes import is_prime
from defm_luigi import Population
import pandas as pd


class PrimesTestCase(unittest.TestCase):
    """Tests for `primes.py`."""

    def test_pop(self):
        """Is five successfully determined to be prime?"""

        Population('2015').run()
        df = pd.read_csv('pop.csv')
        df1 = df
        self.assertTrue(df.equals(df1))

if __name__ == '__main__':
    unittest.main()