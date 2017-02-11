import os
import unittest


def discover_tests():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    start_dir = os.path.join(current_dir, 'tests')

    loader = unittest.TestLoader()
    suite = loader.discover(start_dir, pattern='test_*.py')
    return suite


def main():
    suite = discover_tests()
    runner = unittest.TextTestRunner(
        verbosity=2
    )
    runner.run(suite)


if __name__ == '__main__':
    main()
