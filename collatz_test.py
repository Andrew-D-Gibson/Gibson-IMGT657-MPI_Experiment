import unittest
import os
from mpi_simulator import collatz_sequence_length

class CollatzUnitTest(unittest.TestCase):
    def test_collatz_sequence_length(self):
        test_inputs = [1,2,7,14,19,27]
        desired_outputs = [0,1,16,17,20,111]

        actual_outputs = [collatz_sequence_length(num) for num in test_inputs]
        assert actual_outputs == desired_outputs


if __name__ == '__main__':
    unittest.main()
