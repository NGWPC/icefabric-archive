import pyarrow as pa
import pyiceberg
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, DoubleType, LongType, BinaryType, BooleanType
from pyiceberg.expressions import EqualTo, LessThan, And, GreaterThan, GreaterThanOrEqual
import pyarrow.parquet as pq
from pyiceberg.schema import Schema
import boto3
import s3fs
import re
import os

class IcebergTable(object):
    '''
    Create a Iceberg table per parquet file w/ its inherited schema set.

    Note: Allows for user to have the option to read parquets from S3 or locally. It is okay to expect
    following warning statements throughout process: "Iceberg does not have a dictionary type. <class
    'pyarrow.lib.DictionaryType'> will be inferred as string on read."
    
    '''
    def __init__(self):

        # Generate folder for iceberg catalog
        if not os.path.exists(f'{os.getcwd()}/iceberg_catalog'):
            os.makedirs(f'{os.getcwd()}/iceberg_catalog')

        # Set location of where the iceberg catalog will reside
        os.environ['PYICEBERG_HOME'] = os.getcwd()

        # Initialize namespace to be set for Iceberg catalog
        self.namespace = str()
    
    def read_data_dirs(self, data_dir):
        '''
        Extract the list of parquet directories.
        
        Args:
            data_dir (str): Parent directory of the parquet files.
                            Note: All the ml_auxiliary_data parquet 
                            files are save under same filenames, 
                            but categorized by 'vpuid' conditions.

        Return (list): List of directories associated with each parquet file.
        
        '''
        parquet_list = []
        for folder, subfolders, files in os.walk(data_dir):
            if folder != data_dir:
                for file in files:
                    parquet_list.append(f'{folder}/{file}')
                    
        return parquet_list
        
    def read_data(self, parquet_file_path):
        '''
        Load a single parquet as a Pyarrow table.
        
        Args: 
            parquet_file_path (str): Directory of a single parquet.

    
        Return: A Pyarrow table.
        
        '''
        data = pq.read_table(parquet_file_path)
        
        return data

    def establish_catalog(self, catalog_name, namespace):
        '''
        Creates a new Iceberg catalog.
        
        Args:
            catalog_name (str): Name of the catalog to be created.
                                Default: 'dev' for development catalog
            namespace (str): Name of namespace.

        Return: None
        
        ''' 
        # Establish a new Iceberg catalog & its configuration
        self.catalog = load_catalog(name=catalog_name,
                                    **{'uri': f'sqlite:///iceberg_catalog/{catalog_name}_catalog.db'})
        
        # Establish namespace to be create w/in catalog
        self.namespace = namespace
        if self.namespace not in self.catalog.list_namespaces():
            self.catalog.create_namespace(self.namespace)
        
        return

    def convert_pyarrow_to_iceberg_schema(self, arrow_schema):
        """
        Translate a given Pyarrow schema into a schema acceptable by Iceberg.

        Args:
            arrow_schema (object): Pyarrow schema read from the loaded 
                                   parquet of interest.

        Return (object): Iceberg schema
        
        """
        fields = []
        for idx in range(len(arrow_schema)):
            
            # Extraction of the datatype & name of each schema row
            field_name = arrow_schema.field(idx).name
            arrow_type = arrow_schema.field(idx).type

            # Iceberg datatypes to pyarrow datatypes
            if pa.types.is_int32(arrow_type):
                iceberg_type = LongType()
            elif pa.types.is_string(arrow_type):
                iceberg_type = StringType()
            elif pa.types.is_float64(arrow_type):
                iceberg_type = DoubleType()
            elif pa.types.is_int64(arrow_type):
                iceberg_type = LongType()
            elif pa.types.is_boolean(arrow_type):
                iceberg_type = BooleanType()
            elif pa.types.is_binary(arrow_type):
                iceberg_type = BinaryType()
            elif pa.types.is_dictionary(arrow_type): 
                if pa.types.is_string(arrow_type.value_type):
                    iceberg_type = StringType()
                elif pa.types.is_int32(arrow_type.value_type):
                    iceberg_type = LongType()
            else:
                raise ValueError(f"Unsupported PyArrow type: {arrow_type}")

            # Establish the new schema acceptable to Iceberg
            fields.append(NestedField(field_id=idx + 1,
                                      required=False,
                                      name=field_name,
                                      field_type=iceberg_type))
        # Iceberg schema
        schema = Schema(*fields)
        
        return schema

    def create_table_for_parquet(self, iceberg_tablename, data_table, schema):
        '''
        Convert parquet Pyarrow table to iceberg table & allocate Iceberg catalog under the ./iceberg_catalog directory.

        Args:
            iceberg_tablename (str): Name of the Iceberg table to be created.
            
            data_table (object): Pyarrow table
            
            schema (object): Unique Iceberg schema to be set for the Iceberg table.
            
            namespace (str): Namespace for which the Iceberg table will reside within
                             the Iceberg catalog.

        Return: None
        
        '''
        
        # Create an Iceberg table
        iceberg_table = self.catalog.create_table(identifier=f"{self.namespace}.{iceberg_tablename}",
                                                  schema=schema,
                                                  location=f"{os.environ['PYICEBERG_HOME']}/iceberg_catalog")
        
        # Updates the Iceberg table with data of interest.
        iceberg_table.append(data_table)
        
        return

    def create_table_for_all_parquets(self, parquet_files, app_name='mip-xs'):
        '''
        Convert parquets to Iceberg tables - each w/ their inherited schema.

        Args:
            parquet_files (list): List of directories of the parquet files.

            app_name (str): Application to create Iceberg tables.
                            Options: 'mip-xs' & 'bathymetry_ml_auxiliary'

        Return: None

        Note: The sourced data structures for the data in 'mip-xs' & 
        'bathymetry_ml_auxiliary' S3 buckets differ.
                
        '''
        for idx, parquet_file in enumerate(parquet_files):
            if app_name == 'mip_xs':
                iceberg_tablename = f"{os.path.split(os.path.split(parquet_file)[1])[1].split('.')[0]}"
            
            elif app_name == 'bathymetry_ml_auxiliary':
                iceberg_tablename = f"{os.path.split(os.path.split(parquet_file)[0])[1]}"

            data_table = self.read_data(parquet_file)
            data_pyarrow_schema = data_table.schema
            schema = self.convert_pyarrow_to_iceberg_schema(data_pyarrow_schema)
            self.create_table_for_parquet(iceberg_tablename,
                                          data_table, 
                                          schema)
        return

    def create_table_for_all_s3parquets(self, app_name, bucket_name, profile_name='default'):
        '''
        Convert parquets from S3 to Iceberg tables - each w/ their inherited schema.
    
        Args:
            bucket_name (list): S3 bucket name.

            app_name (str): Application to create Iceberg tables.
                            Options: 'mip_xs' & 'bathymetry_ml_auxiliary'
            
            namespace (str): Namespace for which the Iceberg table will reside within
                             the Iceberg catalog.

            profile_name (str: Profile name declared in the AWS configuration file. 
                               Default: 'default' 
    
        Return: None

        '''
        # Instantiate bucket of interest.
        session = boto3.Session(profile_name=profile_name)
        s3 = session.resource('s3')
        bucket_ob = s3.Bucket(bucket_name)
        pyarrow_table = {}
        for s3obj in bucket_ob.objects.all():

            # For sourcing the preprocessed XS parquets from S3
            if app_name == 'mip_xs' and re.match(r'^xs_data/.*\.parquet$', s3obj.key):
                iceberg_tablename = f"{os.path.split(os.path.split(s3obj.key)[1])[1].split('.')[0]}"
                pyarrow_table[iceberg_tablename]= pq.read_table(f's3://{bucket_name}/{s3obj.key}',
                                        filesystem=s3fs.S3FileSystem())
                
            # For sourcing the bathymetry_ml_auxiliary parquets from S3
            elif app_name == 'bathymetry_ml_auxiliary':
                iceberg_tablename = f"{os.path.split(os.path.split(s3obj.key)[0])[1]}"
                pyarrow_table[iceberg_tablename]= pq.read_table(f's3://{bucket_name}/{s3obj.key}',
                                        filesystem=s3fs.S3FileSystem())
                
        # Xforming each unique parquet to an iceberg table    
        for key, data_table in pyarrow_table.items():
            data_pyarrow_schema = data_table.schema
            schema = self.convert_pyarrow_to_iceberg_schema(data_pyarrow_schema)
            self.create_table_for_parquet(key,
                                          data_table, 
                                          schema)
        return
            
        
# if __name__ == '__main__': 

    # Instantiate class
    #obj = IcebergTable()
    
    # ======= For ml_auxiliary_data application =======

    ## Extract list of parquets
    #parquet_list = obj.read_data_dirs(data_dir=f'{os.getcwd()}/data/bathymetry/ml_auxiliary_data/')
    
    ## Establish new Iceberg catalog
    #obj.establish_catalog(catalog_name='bathymetry_ml_auxiliary_dev',
    #                       namespace='bathymetry_ml_auxiliary')
    
    ## Generate tables w/in the new Iceberg catalog for all parquets detected 
    #obj.create_table_for_all_parquets(parquet_list, 
    #                                  app_name='bathymetry_ml_auxiliary')
    
    # ======= For mip xs data application =======

    # Extract list of parquets
    #file_list = obj.read_data_dirs(data_dir=f'{os.getcwd()}/xs_data/')
    #parquet_list = [file for file in file_list if file.endswith('.parquet')]

    #Establish new Iceberg catalog for XS data
    #obj.establish_catalog(catalog_name='xs',
    #                      namespace='mip')

    # Generate tables w/in the new Iceberg catalog for all parquets detected
    #obj.create_table_for_all_parquets(parquet_list, app_name='mip-xs')