from mage_ai.data_cleaner.transformer_actions.base import BaseAction
from mage_ai.data_cleaner.transformer_actions.constants import ActionType, Axis
from mage_ai.data_cleaner.transformer_actions.utils import build_transformer_action
from pandas import DataFrame
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def execute_transformer_action(df: DataFrame, *args, **kwargs) -> DataFrame:
    """
    Execute Transformer Action: ActionType.FILTER

    Docs: https://docs.mage.ai/guides/transformer-blocks#filter
    """
    #df.GoldsteinScale = df.GoldsteinScale.astype('float')
    map_columns = ['GLOBALEVENTID',
                   'SQLDATE',
                   'MonthYear',
                   'Year',
                   'Actor1Name',
                   'Actor1CountryCode',
                   'Actor1Geo_Lat',
                   'Actor1Geo_Long',
                   'Actor2Name',
                   'Actor2CountryCode',
                   'Actor2Geo_Lat',
                   'Actor2Geo_Long'
                   'EventCode',
                   'GoldsteinScale',
                   'AvgTone']
    
    df = df.dropna() 
    df = df.astype( {
               'IsRootEvent': bool,		
               'QuadClass': int,
               'GoldsteinScale': float,	
                'NumMentions': int,	
                'NumSources': int,	
                'NumArticles': int,	
                'AvgTone': float,	
                'Actor1Geo_Lat': float	,
                'Actor1Geo_Long': float,	
                'Actor2Geo_Lat': float	,
                'Actor2Geo_Long': float,	
                'ActionGeo_Lat'	: float,
                'ActionGeo_Long': float,	
    })
    print(df.dtypes)
    action = build_transformer_action(
        df,
        action_type=ActionType.FILTER,
        axis=Axis.ROW,
        action_code='GoldsteinScale > 3', 
    )

    return BaseAction(action).execute(df)


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
