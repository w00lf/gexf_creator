import os
import sys
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))

def fixtures_path(file_name):
    return os.path.abspath(os.path.join(os.path.dirname(__file__), 'fixtures', file_name))
