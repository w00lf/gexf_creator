import unittest
import unittest.mock
import context
import os
import main

class CategoriesTest(unittest.TestCase):
  def setUp(self):
    self.client = main.MainClient(
        input_file='sample_input.csv', version='test', cloud_storage_client=self.mock_cloud_storage(), creator='creator', name='name')

  def mock_download_string(self, file_to_download):
    return '\r\n'.join(open(context.fixtures_path(file_to_download), 'r').readlines())

  def truphy_lambda(self):
    return lambda *args: True

  def mock_cloud_storage(self):
    truphy = self.truphy_lambda()
    return unittest.mock.Mock(
            merge_files_into_one=truphy,
            upload_from_file=truphy,
            upload_string=truphy,
            download_to_file=truphy,
            download_string=self.mock_download_string,
            file_exists=truphy,
            delete_file=truphy)

  def test_main(self):
    self.assertTrue(self.client.start())

if __name__ == '__main__':
    unittest.main()
