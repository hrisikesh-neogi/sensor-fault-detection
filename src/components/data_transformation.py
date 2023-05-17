import sys
from typing import Union
import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from collections import namedtuple
from src.constant import *
from src.exception import VisibilityException
from src.logger import logging
from src.utils.main_utils import MainUtils
from dataclasses import dataclass

@dataclass
class DataTransformationConfig:
    data_transformation_dir=os.path.join(artifact_folder,'data_transformation')
    transformed_train_file_path=os.path.join(data_transformation_dir, 'train.npy')
    transformed_test_file_path=os.path.join(data_transformation_dir, 'test.npy') 
    transformed_object_file_path=os.path.join( data_transformation_dir, 'preprocessing.pkl' )






class DataTransformation:
    def __init__(self,
                 valid_data_dir):
       
        self.valid_data_dir = valid_data_dir

        self.data_transformation_config = DataTransformationConfig()


        self.utils =  MainUtils()
        
    
    def drop_schema_columns(self, dataframe:pd.DataFrame) -> pd.DataFrame:
        """
        Method Name :   drop_schema_columns
        Description :   This method reads the schema.yml file and drops the column in th dataset based on the schema given. 
        
        Output      :   a pd.DataFrame dropping the schema columns
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            _schema_config = self.utils.read_schema_config_file()
            df = dataframe.drop(columns =  _schema_config["drop_columns"])

            return df
        except Exception as e:
            raise VisibilityException(e,sys)

    @staticmethod
    def get_merged_batch_data(valid_data_dir:str) -> pd.DataFrame:
        """
        Method Name :   get_merged_batch_data
        Description :   This method reads all the validated raw data from the valid_data_dir and returns a pandas DataFrame containing the merged data. 
        
        Output      :   a pandas DataFrame containing the merged data 
        On Failure  :   Write an exception log and then raise an exception
        
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            raw_files = os.listdir(valid_data_dir)
            csv_data = []
            for filename in raw_files:
                data = pd.read_csv(os.path.join(valid_data_dir, filename))
                csv_data.append(data)

            merged_data = pd.concat(csv_data)

            return merged_data
        except Exception as e:
            raise VisibilityException(e,sys)
        
    def apply_outliers_capping(self,dataframe:pd.DataFrame):
        """
            Method Name :   apply_outliers_capping
            Description :   This method reduces the outliers
            
            Output      :   a pd.DataFrame
            On Failure  :   Write an exception log and then raise an exception
            
            Version     :   1.2
            Revisions   :   moved setup to cloud
        """


        try:

            outliers_columns = self.utils.read_schema_config_file()['outlier_columns']

            
            for column in outliers_columns:

                percentile25 = dataframe[column].quantile(0.25)
                percentile75 = dataframe[column].quantile(0.75)
                iqr = percentile75 - percentile25
                upper_limit = percentile75 + 1.5 * iqr
                lower_limit = percentile25 - 1.5 * iqr
                dataframe.loc[(dataframe[column]>upper_limit), column]= upper_limit
                dataframe.loc[(dataframe[column]<lower_limit), column]= lower_limit   
            
            

            return dataframe

        except Exception as e:
            raise VisibilityException(e,sys)



             
    def initiate_data_transformation(self) :
        """
            Method Name :   initiate_data_transformation
            Description :   This method initiates the data transformation component for the pipeline 
            
            Output      :   data transformation artifact is created and returned 
            On Failure  :   Write an exception log and then raise an exception
            
            Version     :   1.2
            Revisions   :   moved setup to cloud
        """

        logging.info(
            "Entered initiate_data_transformation method of Data_Transformation class"
        )

        try:
            dataframe = self.get_merged_batch_data(valid_data_dir=self.valid_data_dir)
            
            dataframe = self.drop_schema_columns(dataframe=dataframe)
            
            dataframe = self.apply_outliers_capping(dataframe)

            X = dataframe.drop(columns= TARGET_COLUMN)
            y = dataframe[TARGET_COLUMN]
            
            X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.2 )



            preprocessor = StandardScaler()

            X_train_scaled =  preprocessor.fit_transform(X_train)
            X_test_scaled  =  preprocessor.transform(X_test)


            preprocessor_path = self.data_transformation_config.transformed_object_file_path
            os.makedirs(os.path.dirname(preprocessor_path), exist_ok= True)
            self.utils.save_object(preprocessor_path,
                        obj= preprocessor)

            train_arr = np.c_[X_train_scaled, np.array(y_train) ]
            test_arr = np.c_[ X_test_scaled, np.array(y_test) ]

            return (train_arr, test_arr, preprocessor_path)
        

        except Exception as e:
            raise VisibilityException(e, sys) from e
