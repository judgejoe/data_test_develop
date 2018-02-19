import unittest
import lxml 
import pandas as pd
import numpy as np
import os
import sys

sys.path.append('../')
import etl

class TestLoad(unittest.TestCase):
 
    def setUp(self):
        self.csv_filename = 'test.csv'
        self.empty_filename = 'test_data/empty.xml'
        self.filename = 'test_data/test_listings.xml'
        self.context = '/Listings/Listing'
        self.output_columns = ['MlsId', 
                               'MlsName',
                               'DateListed',
                               'StreetAddress', 
                               'Price', 
                               'Bedrooms', 
                               'Bathrooms',
                               'Appliances', 
                               'Rooms', 
                               'Description']
        self.json = {"MlsId":{"0":"14799273","1":"14802845","2":"14802846"},"MlsName":{"0":"CLAW","1":"CLAW","2":"CLAP"},"DateListed":{"0":"2014-10-03 00:00:00","1":"2014-10-17 00:00:00","2":"2014-10-17 00:00:00"},"StreetAddress":{"0":"0 Castro Peak Mountainway","1":"0 SADDLE PEAK RD","2":"0 SADDLE PEAK RD"},"City":{"0":"Malibu","1":"Malibu","2":"Malibu"},"State":{"0":"CA","1":"CA","2":"CA"},"Zip":{"0":"90265","1":"90290","2":"90290"},"Price":{"0":535000.00,"1":200000.00,"2":200000.00},"Bedrooms":{"0":0.0,"1":0.0,"2":0.0},"Bathrooms_raw":{"0":3.5,"1":np.nan,"2":np.nan},"FullBathrooms":{"0":3.0,"1":2.0,"2":np.nan},"HalfBathrooms":{"0":1.0,"1":1.0,"2":np.nan},"ThreeQuarterBathrooms":{"0":np.nan,"1":np.nan,"2":np.nan},"Full_Description":{"0":"Enjoy amazing ocean and island views from this 10+ acre parcel situated in a convenient and peaceful area of the Santa Monica mountains. Just minutes from beaches or the 101, Castro Peak is located off of Latigo canyon in an area sprinkled with vineyards, ranches and horse properties. A paved road leads you to the site which features considerable useable land and multiple development areas. This is an area of new development. Build your dream.","1":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved private road leads you almost to the site. This lot has development challenges - not for the faint of heart. Property has been owned by the same family for over 40 years. Reports and information is limited.","2":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved private road leads you almost to the site. This lot has development challenges - not for the faint of heart. Property has been owned by the same family for over 40 years. Reports and information is limited."},"Appliances":{"0":np.nan,"1":np.nan,"2":np.nan},"Rooms":{"0":np.nan,"1":np.nan,"2":np.nan},"bathrooms_calc":{"0":4,"1":3,"2":0},"Bathrooms":{"0":3.5,"1":3.0,"2":np.nan},"Description":{"0":"Enjoy amazing ocean and island views from this 10+ acre parcel situated in a convenient and peaceful area of the Santa Monica mountains. Just minutes from beaches or the 101, Castro Peak is located of","1":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved","2":"Spectacular views from this 4+ acre property perched on the ridge between PCH and the Valley. Two APNs - 4438-034-037 and 031 being sold together. Plus, there is a lot next door for sale too! A, paved"}} 

    def tearDown(self):
        os.remove(self.csv_filename)

    def test_file_existence(self):
        df = pd.DataFrame(self.json)
        etl.load_csv(df, self.csv_filename, self.output_columns)
        retval = os.system('ls ' + self.csv_filename)
        self.assertEqual(retval, 0)

    def test_file_length(self):
        df = pd.DataFrame(self.json)
        etl.load_csv(df, self.csv_filename, self.output_columns)
        p = os.popen('wc -l ' + self.csv_filename)
        linecount = int(p.read().split(" ")[0])
        p.close()
        self.assertEqual(linecount,4)
   
    def test_data_equality(self):
        df = pd.DataFrame(self.json)
        etl.load_csv(df, self.csv_filename, self.output_columns)
        df2 = pd.read_csv(self.csv_filename,index_col='MlsId')
        df = df[self.output_columns]

        df.set_index('MlsId', inplace=True)
        df.index = df.index.map(int) 

        pd.util.testing.assert_frame_equal(df, df2, check_dtype=False, check_index_type=False, check_column_type=False, check_names=False, check_less_precise=1)

if __name__ == '__main__':
    unittest.main()
