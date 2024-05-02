if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from urllib.request import urlopen
from io import BytesIO, StringIO
import zipfile
import pandas as pd


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    
    columns = ['GLOBALEVENTID',	
               'SQLDATE',
               'MonthYear',
               'Year',
               'FractionDate',
               'Actor1Code',
               'Actor1Name',
               'Actor1CountryCode',	
               'Actor1KnownGroupCode',	
               'Actor1EthnicCode',	
               'Actor1Religion1Code',	
               'Actor1Religion2Code',	
               'Actor1Type1Code',	
               'Actor1Type2Code',
               'Actor1Type3Code',	
               'Actor2Code',	
               'Actor2Name',	
               'Actor2CountryCode',
               'Actor2KnownGroupCode',	
               'Actor2EthnicCode',	
               'Actor2Religion1Code',	
               'Actor2Religion2Code',	
               'Actor2Type1Code',	
               'Actor2Type2Code',	
               'Actor2Type3Code',	
               'IsRootEvent',	
               'EventCode',	
               'EventBaseCode',	
               'EventRootCode',	
               'QuadClass',
               'GoldsteinScale',	
            'NumMentions',	
                'NumSources',	
                'NumArticles',	
                'AvgTone',	
                'Actor1Geo_Type',	
                'Actor1Geo_FullName',	
                'Actor1Geo_CountryCode',	
                'Actor1Geo_ADM1Code',	
                'Actor1Geo_Lat'	,
                'Actor1Geo_Long',	
                'Actor1Geo_FeatureID',	
                'Actor2Geo_Type'	,
                'Actor2Geo_FullName',	
                'Actor2Geo_CountryCode'	,
                'Actor2Geo_ADM1Code',	
                'Actor2Geo_Lat'	,
                'Actor2Geo_Long',	
                'Actor2Geo_FeatureID',	
                'ActionGeo_Type'	,
                'ActionGeo_FullName',	
                'ActionGeo_CountryCode'	,
                'ActionGeo_ADM1Code'	,
                'ActionGeo_Lat'	,
                'ActionGeo_Long',	
                'ActionGeo_FeatureID',	
                'DATEADDED'	,
                'SOURCEURL']
    columns_types = {
               'IsRootEvent': bool,		
               'QuadClass': pd.Int32Dtype(),
               'GoldsteinScale': pd.Float64Dtype(),	
                'NumMentions': pd.Int32Dtype(),	
                'NumSources': pd.Int32Dtype(),	
                'NumArticles': pd.Int32Dtype(),	
                'AvgTone': pd.Float64Dtype(),	
                'Actor1Geo_Lat': pd.Float64Dtype()	,
                'Actor1Geo_Long': pd.Float64Dtype(),	
                'Actor2Geo_Lat': pd.Float64Dtype()	,
                'Actor2Geo_Long': pd.Float64Dtype(),	
                'ActionGeo_Lat'	: pd.Float64Dtype(),
                'ActionGeo_Long': pd.Float64Dtype(),	
    }
    """
    request = urlopen(f"{kwargs['configuration'].get('url')}/20240419.export.CSV.zip")
    
    zip_file = BytesIO(request.read())

    try:
        with zipfile.ZipFile(zip_file, mode="r") as archive:
            for info in archive.infolist():
                file_name_info = info.filename
            file_stream = archive.read(file_name_info).split(b'\n')
    except zipfile.BadZipFile as error:
        print(error)

    data = pd.DataFrame(data=[line.decode('utf-8').split('\t') for line in file_stream],
                        columns=columns)

    print(data.dtypes)
    
    return data
    """
    url = f"{kwargs['configuration'].get('url')}/20240419.export.CSV.zip"
    df = pd.read_csv(url, sep="\t",compression='zip', names=columns, dtype=columns_types)
    return df




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

if __name__ == '__main__':
    load_data(**{'configuration':{'url':'http://data.gdeltproject.org/events'}})