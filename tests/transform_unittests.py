import unittest
import lxml 
import pandas as pd
import sys

sys.path.append('../')
import etl

class TestTransform(unittest.TestCase):
 
    def setUp(self):
        self.empty_filename = 'test_data/empty.xml'
        self.filename = 'test_data/test_listings.xml'
        self.context = '/Listings/Listing'
        self.columns = [{'name' : 'MlsId',                 'xpath' : 'string(ListingDetails/MlsId/text())',               'vtype' : 'scalar'},
                        {'name' : 'MlsName',               'xpath' : 'string(ListingDetails/MlsName/text())',             'vtype' : 'scalar'},
                        {'name' : 'DateListed',            'xpath' : 'string(ListingDetails/DateListed/text())',          'vtype' : 'scalar'},
                        {'name' : 'StreetAddress',         'xpath' : 'string(Location/StreetAddress/text())',             'vtype' : 'scalar'},
                        {'name' : 'City',                  'xpath' : 'string(Location/City/text())',                      'vtype' : 'scalar'},
                        {'name' : 'State',                 'xpath' : 'string(Location/State/text())',                     'vtype' : 'scalar'},
                        {'name' : 'Zip',                   'xpath' : 'string(Location/Zip/text())',                       'vtype' : 'scalar'},
                        {'name' : 'Price',                 'xpath' : 'string(ListingDetails/Price/text())',               'vtype' : 'scalar'},
                        {'name' : 'Bedrooms',              'xpath' : 'number(BasicDetails/Bedrooms/text())',              'vtype' : 'scalar'},
                        {'name' : 'Bathrooms_raw',         'xpath' : 'number(BasicDetails/Bathrooms/text())',             'vtype' : 'scalar'},
                        {'name' : 'FullBathrooms',         'xpath' : 'number(BasicDetails/FullBathrooms/text())',         'vtype' : 'scalar'},
                        {'name' : 'HalfBathrooms',         'xpath' : 'number(BasicDetails/HalfBathrooms/text())',         'vtype' : 'scalar'},
                        {'name' : 'ThreeQuarterBathrooms', 'xpath' : 'string(BasicDetails/ThreeQuarterBathrooms/text())', 'vtype' : 'scalar'},
                        {'name' : 'Full_Description',      'xpath' : 'string(BasicDetails/Description/text())',           'vtype' : 'scalar'},
                        {'name' : 'Appliances',            'xpath' : 'RichDetails/Appliances/*/text()',                   'vtype' : 'list'},
                        {'name' : 'Rooms',                 'xpath' : 'RichDetails/Rooms/*/text()',                        'vtype' : 'list'}]

        self.json = {"MlsId":{"0":"14799273","1":"14802845","2":"14802846"},"MlsName":{"0":"CLAW","1":"CLAW","2":"CLAP"},"DateListed":{"0":"2014-10-03 00:00:00","1":"2014-10-17 00:00:00","2":"2014-10-17 00:00:00"},"StreetAddress":{"0":"0 Castro Peak Mountainway","1":"0 SADDLE PEAK RD","2":"0 SADDLE PEAK RD"},"City":{"0":"Malibu","1":"Malibu","2":"Malibu"},"State":{"0":"CA","1":"CA","2":"CA"},"Zip":{"0":"90265","1":"90290","2":"90290"},"Price":{"0":"535000.00","1":"200000.00","2":"200000.00"},"Bedrooms":{"0":0.0,"1":0.0,"2":0.0},"Bathrooms_raw":{"0":3.5,"1":None,"2":None},"FullBathrooms":{"0":3.0,"1":2.0,"2":None},"HalfBathrooms":{"0":1.0,"1":1.0,"2":None},"ThreeQuarterBathrooms":{"0":None,"1":None,"2":None},"Full_Description":{"0":"Enjoy amazing ocean and island views from this 10+ acre parcel situated in a convenient and peaceful area of the Santa Monica mountains. Just minutes from beaches or the 101, Castro Peak is located off of Latigo canyon in an area sprinkled with vineyards, ranches and horse properties. A paved road leads you to the site which features considerable useable land and multiple development areas. This is an area of new development. Build your dream.","1":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved private road leads you almost to the site. This lot has development challenges - not for the faint of heart. Property has been owned by the same family for over 40 years. Reports and information is limited.","2":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved private road leads you almost to the site. This lot has development challenges - not for the faint of heart. Property has been owned by the same family for over 40 years. Reports and information is limited."},"Appliances":{"0":None,"1":None,"2":None},"Rooms":{"0":None,"1":None,"2":None}}

    def test_bathrooms_field(self):
        df = pd.DataFrame(self.json)
        df = etl.transform(df)
        self.assertEqual(df.iloc[0]['Bathrooms'], 3.5)

    def test_bathrooms_calc_field(self):
        df = pd.DataFrame(self.json)
        df = etl.transform(df)
        self.assertEqual(df.iloc[1]['Bathrooms'], 3)

    def test_bathrooms_field_na(self):
        df = pd.DataFrame(self.json)
        df = etl.transform(df)
        self.assertTrue(pd.isnull(df.iloc[2]['Bathrooms']))

    def test_description_trunc(self):
        df = pd.DataFrame(self.json)
        df = etl.transform(df)
        self.assertEqual(len(df.iloc[0]['Description']),200)
''' 
    def test_empty_files(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','ctype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','ctype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','ctype':'list'}]
        try:
            self.assertRaises(lxml.etree.XMLSyntaxError, etl.extract_xml(self.empty_filename, columns, self.context))
        except lxml.etree.XMLSyntaxError:
            pass
 
    def test_list_extraction(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','ctype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','ctype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','ctype':'list'}]
        df = etl.extract_xml(self.filename, columns, self.context)
        self.assertEqual(df.iloc[0]['grandchildnode3'], "3,4")
        self.assertEqual(df.iloc[1]['grandchildnode3'], "3")

    def test_scalar_extraction(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','ctype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','ctype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','ctype':'list'}]
        df = etl.extract_xml(self.filename, columns, self.context)
        self.assertEqual(df.iloc[0]['grandchildnode1'], "1")

    def test_list_type_error(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','ctype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','ctype':'scalar'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','ctype':'scalar'}]
        try:
            self.assertRaises(TypeError, etl.extract_xml(self.filename, columns, self.context))
        except TypeError:
            pass

    def test_scalar_type_error(self):
        columns  = [{'name':'grandchildnode1','xpath':'string(grandchildnode1/text())','ctype':'scalar'},
                    {'name':'grandchildnode2','xpath':'string(grandchildnode2/text())','ctype':'list'},
                    {'name':'grandchildnode3','xpath':'grandchildnode3/*/text()','ctype':'list'}]
        try:
            self.assertRaises(TypeError, etl.extract_xml(self.filename, columns, self.context))
        except TypeError:
            pass
'''
if __name__ == '__main__':
    unittest.main()
