import unittest
import lxml 
import pandas as pd
import sys

sys.path.append("../")
import etl

class TestExtract(unittest.TestCase):
 
    def setUp(self):
        self.empty_filename = '../test_data/empty.xml'
        self.filename = '../test_data/test.xml'
        self.context = '/rootnode/child'

    def test_line_numbers(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        self.assertEqual(len(etl.extract_xml(self.filename, columns, self.context)), 3)
 
    def test_bad_context(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        self.assertEqual(len(etl.extract_xml(self.filename, columns, 'nonexistant-context')), 0)

    def test_empty_files(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        try:
            self.assertRaises(lxml.etree.XMLSyntaxError, etl.extract_xml(self.empty_filename, columns, self.context))
        except lxml.etree.XMLSyntaxError:
            pass
 
    def test_list_extraction(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        df = etl.extract_xml(self.filename, columns, self.context)
        self.assertEqual(df.iloc[0]['grandchildnode3'], "3,4")
        self.assertEqual(df.iloc[1]['grandchildnode3'], "7")
        self.assertTrue(pd.isnull(df.iloc[2]['grandchildnode3']) )

    def test_scalar_extraction(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        df = etl.extract_xml(self.filename, columns, self.context)
        self.assertEqual(df.iloc[0]['grandchildnode1'], "1")

    def test_list_type_error(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'scalar'}]
        try:
            self.assertRaises(TypeError, etl.extract_xml(self.filename, columns, self.context))
        except TypeError:
            pass

    def test_scalar_type_error(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','vtype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','vtype':'list'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','vtype':'list'}]
        try:
            self.assertRaises(TypeError, etl.extract_xml(self.filename, columns, self.context))
        except TypeError:
            pass

if __name__ == '__main__':
    unittest.main()
