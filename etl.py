import lxml
from lxml import etree
import pandas as pd
import numpy as np

def get_rows(listings, cols):
    """
    A helper function for the extract_xml function. It returns a two dimenionsal array represnting the data set
    :param listings: collection of lxml objects represnting listings
    :param cols: column spec (see extract_xml for details)
    :returns: a two dimensional array of results. The first dimension are the rows. The second dimension are the
              columns
    :raises: TypeError
    """
    rows = []
    # Loop over each listing
    for child in listings:
        row = []
        # For each column that has been specified
        for col in cols:
            # Extract the element using the column's xpath
            value = child.xpath(col['xpath'])
            if col['vtype'] == 'scalar':
                # if it's a scalar, just append it
                if type(value) != lxml.etree._ElementUnicodeResult and \
                   type(value) != lxml.etree._ElementStringResult and \
                   type(value) != float and \
                   type(value) != int:
                    raise TypeError("vtype doesn't match value. col name: " +col['name'] + " col vtype: " + col['vtype'] + " value: " + str(type(value)))
                row.append(value if value != '' else np.nan)
            elif col['vtype'] == 'list':
                # if it's a list of sub nodes, grab them, join them with a comma,
                # then insert as a string
                if type(value) != list:
                    raise TypeError("vtype doesn't match value. col name: " +col['name'] + " col vtype: " + col['vtype'] + " value: " + str(type(value)))
                row.append(",".join(value) if len(value) > 0 else np.nan)

        if(len(row) > 0):
            rows.append(row)
    return rows



def extract_xml(filename, columns, path_to_listings):
    """
    The extract function in our ETL process. It takes an xml file as input and returns a Pandas Dataframe.
    This function was designed to be flexible enough to support multiple XML Schema. The Dataframe returned 
    will be consist across schema as long as the proper column spec is provided for that schema.
    :param filename: url or filename
    :param columns: The column spec is an array of dictionaries. Each dictionary must have the following fields:
                        name - the column name for the Dataframe
                        xpath - the XPath statement used to extract the data element from the XML. For scalars the
                                XPath statement must return a single scalar value. For lists, the XPath statement must
                                return a list.
                        vtype - collection type. Must be either scalar or list. Scalars will be inserted into the 
                                returned Dataframe as they are read from the file. Their type will be inferred by 
                                Pandas. Lists will have their elements concatenated with a comma and then be inserted 
                                into the dataframe as a string (dtype object)
    :path_to_listings: the XPath to the listing records i.e. if the XML doc has the structure
                            <Listings>
                                <Listing>
                                    <tag1>foo</tag>
                                    <tag2>bar</tag>
                                </Listing>
                            </Listings>
                        the path_to_listings would be '/Listings/Listing'.
    :returns: A Pandas Dataframe with the column names from the spec provided
    :raises TypeError
    """
        
    tree = etree.parse(filename)
    root = tree.getroot()
    rows = get_rows(root.xpath(path_to_listings), columns)

    return pd.DataFrame(rows,columns=[col['name'] for col in columns])



def bathroom_counter(row):
    """
    Helper function to decide the number of bathrooms for a listing. Many (all?) listings do not have a bathroom count
    but they do have counts for the full, quarter and half baths which are totaled in a separate column created during
    the transform stage.     
    :param: row - a Pandas series representing a row of data from the apply function
    :returns: a bathroom count 
    :raises: None
    """
    
    if pd.isnull(row['Bathrooms_raw']) and row['bathrooms_calc'] == 0:
        return np.nan
    elif not pd.isnull(row['Bathrooms_raw']):
        return row['Bathrooms_raw']
    else:
        return row['bathrooms_calc']



def transform(df):
    """
    This is the transform function for the Zillow data set. It transforms a data frame from the raw form extracted in
    the extract_xml function into the form that the CSV calls for.
    :param: df -- the dataframe to transform. This dataframe MUST have the following columns:
                     StreetAddress
                     Bathrooms_raw - the bathrooms field extracted directly from the XML
                     FullBathrooms
                     HalfBathrooms
                     ThreeQuarterBathrooms
                     Full_Description
                     Appliances
                     Rooms
    :returns: a transformed dataframe. 
                NaNs filled in with '' for all string values
                'Bathrooms' filled in with either the value directly from the XML or a computed value where 
                            each full, quarter and half bath counts as ONE bathroom
                'Description' contains the Description truncated to 200 characters
    :raises: None
    """
    # Fill in nan values
    #df['Rooms']               = df['Rooms'].fillna('')
    #df['Appliances']          = df['Appliances'].fillna('')
    df['Full_Description']    = df['Full_Description'].fillna('')
    df['StreetAddress']       = df['StreetAddress'].fillna('')
    
    # Calculate the number of bathroooms based on the half, full and quarter bath fields. Our logic will be to sum 
    # them together to get the total of bathrooms. Note that each full, quarter and half bath counts as ONE bathroom. 
    # In other words, a bathroom is any room with toilet. This is largely for simplicity, since counting bathrooms 
    # fractionally would give ambiguous results (i.e. 2 half baths are two separate bathrooms but counted fractionally
    # would be 1 bathroom)

    df['bathrooms_calc']      = df['HalfBathrooms'].fillna(0).astype('int') + \
                                df['FullBathrooms'].fillna(0).astype('int') + \
                                df['ThreeQuarterBathrooms'].fillna(0).astype('int')

    df['bathrooms_calc'] = df['bathrooms_calc'].apply(lambda x: x if x !=0 else np.nan)
    
    # Fill in the final bathrooms field with either the value given in the bathrooms field
    # or, if that is missing, the calculated field from above
    df['Bathrooms']           = df.apply(bathroom_counter, axis=1)
    
    # Truncate the description to 200 characters
    df['Description']         = df['Full_Description'].apply(lambda x: x[0:200])
    
    return df



def load_csv(df, filename, cols):
    """
    This the load stage of our ETL pipeline. It takes a DataFrame and writes a CSV.
    :param: df - the Dataframe to load
    :param: filename - the filename to write to
    :param: cols - the columns to pull from the Dataframe
    :returns: None
    :raises: None
    """
    df.to_csv(filename, index=False, columns=cols)



if __name__ == "__main__":
    xml_filename = 'http://syndication.enterprise.websiteidx.com/feeds/BoojCodeTest.xml' 
    csv_filename = 'zillow.csv'
    output_columns = [  'MlsId', 
                        'MlsName',
                        'DateListed',
                        'StreetAddress', 
                        'Price', 
                        'Bedrooms', 
                        'Bathrooms',
                        'Appliances', 
                        'Rooms', 
                        'Description']
    ''' 
    The column spec used by the extract function. This is how our extract process will know 
        a) the name of the columns to use in the data frame
        b) the XPath statement to find the data element within each 'row', 
        c) the type of each column, and d) whether 
        d) whether the XPath returns a scalar or list -- these types will need to be processed separately
    '''

    columns = [{'name' : 'MlsId',                 'xpath' : 'string(ListingDetails/MlsId/text())',               'vtype' : 'scalar'},
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


    a = extract_xml(xml_filename, columns, '/Listings/Listing')
    b = transform(a)
    load_csv(b, csv_filename, output_columns)

