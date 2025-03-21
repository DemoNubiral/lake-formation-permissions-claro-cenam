import boto3
import os
import json
from botocore.exceptions import ClientError
import ast

#### Adicionar a nivel de columna ###
class Permissions:
    def __init__(self, path_dir):
        self.path_dir = path_dir
        self.lakeformation = boto3.client('lakeformation')

    def grant_permissions(self, role_arn, lf_tags, permissions, permissions_with_grant_option, resource_type, catalog_id, action_flag):
        try:
            resource={
                'LFTagPolicy': {
                    'CatalogId': catalog_id,  
                    'ResourceType': resource_type ,     
                    'Expression': lf_tags
                }
            }
            # Crea la política para otorgar permisos utilizando LF-tags
            if action_flag=='grant':
                response = self.lakeformation.grant_permissions(
                    Principal={
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    Resource=resource,
                    Permissions=permissions,
                    PermissionsWithGrantOption=permissions_with_grant_option
                )
                print(f"LF-tags otorgado exitosamente: {response}")
            elif action_flag=='revoke':
                response = self.lakeformation.revoke_permissions(
                    Principal={
                        'DataLakePrincipalIdentifier': role_arn
                    },
                    Resource=resource,
                    Permissions=permissions,
                    PermissionsWithGrantOption=permissions_with_grant_option
                )
                print(f"LF-tags revocado exitosamente: {response}")
        except Exception as e:
            return str(e)
        
    
    def create_lf_tags(self, tag_key, tag_values):
        try:
            # Crea una etiqueta LF
            response = self.lakeformation.create_lf_tag(
                TagKey=tag_key,
                TagValues=tag_values
            )

            return response

        except Exception as e:
            return str(e)


    def assign_lf_tags(self, database_name, table_name, catalog_id, assign_lf_tags):
        try:
            # Asigna etiquetas LF a recursos
            for item in assign_lf_tags:
                    response = self.lakeformation.add_lf_tags_to_resource(
                        CatalogId=catalog_id,  

                        Resource={
                            'Table': {
                                'DatabaseName': database_name,
                                'Name': table_name,
                            }
                        },
                        LFTags=[
                            {
                                'CatalogId': catalog_id,
                                'TagKey': item['TagKey'],
                                'TagValues': item['TagValues']
                            },
                        ]
                    )

            return response
        except Exception as e:
            return str(e)
        

    def assign_lf_tags_columns(self, database_name, table_name, catalog_id, assign_lf_tags, column_names=None):

        if isinstance(assign_lf_tags, str):
            lf_tags = json.loads(assign_lf_tags)
        elif isinstance(assign_lf_tags, list):
            lf_tags = assign_lf_tags
        else:
            raise TypeError("assign_lf_tags debe ser una lista o una cadena JSON.")     

        if isinstance(column_names, str):
            try:
                column_names = ast.literal_eval(column_names)
            except (ValueError, SyntaxError):
                raise ValueError("COLUMN_NAME no es una lista válida ni se pudo convertir.")
        elif not isinstance(column_names, list):
            raise ValueError("column_names debe ser una lista después de la conversión.")
            
        column_names = [col.strip().strip("'").strip('"') for col in column_names] 

        column_tags = {
            column_name: [{"CatalogId": catalog_id, "TagKey": tag["TagKey"], "TagValues": tag["TagValues"]}]
            for column_name in column_names
            for tag in lf_tags
        }

        for column_name, lf_tags in column_tags.items():
            try:
                resource = {
                    "TableWithColumns": {
                        "DatabaseName": database_name,
                        "Name": table_name,
                        "ColumnNames": [column_name],
                    }
                }

                response = self.lakeformation.add_lf_tags_to_resource(
                    Resource=resource,
                    LFTags=lf_tags
                )

                print(f"LF-Tags asignadas exitosamente a la columna '{column_name}': {response}")
            except ClientError as e:
                print(f"Error asignando LF-Tags a la columna '{column_name}': {e}")
        
    
    def read_file_env(self, file_path):
        
        with open(file_path, 'r') as file:
            env_data_list = [] 
            env_data = {} 

            for line in file:
                if not line.strip() or line.startswith("#"):
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                
                if key in ["TAG_VALUES", "ASSIGN_TAG", "LF_TAGS", "PERMISSIONS", "PERMISSIONS_WITH_GRANT_OPTION"]:
                    try:
                        value = json.loads(value)
                    except json.JSONDecodeError:
                        pass  
                
                env_data[key] = value            

                if key == "FLAG_PERMISSIONS":
                    env_data_list.append(env_data)
                    env_data = {}  

            if env_data:
                env_data_list.append(env_data)

            return env_data_list

    def read_all_env_files(self):

        all_configs = [] 
        for filename in os.listdir(self.path_dir):
            if filename.endswith(".env"): 
                file_path = os.path.join(self.path_dir, filename)
                all_configs.extend(self.read_file_env(file_path)) 
        return all_configs
    

    def create_data_cells_filter(self, catalog_id, database_name, table_name, filter_name, row_filter, columns_name,  excluded_columns=None, version_id=None
    ):

        # Si column_names parece una lista en texto, conviértela a una lista real
        if isinstance(columns_name, str):
            try:
                columns_name = ast.literal_eval(columns_name)
            except (ValueError, SyntaxError):
                raise ValueError("COLUMN_NAME no es una lista válida ni se pudo convertir.")
        elif not isinstance(columns_name, list):
            raise ValueError("column_names debe ser una lista después de la conversión.")
            
        columns_name = [col.strip().strip("'").strip('"') for col in columns_name] 

        print(f"DEBUG - Tipo de excluded_columns antes de la validación: {type(excluded_columns)}, Valor: {excluded_columns}")

        if excluded_columns in [None, "None", "null", "NULL", ""]:
            excluded_columns = None
        elif isinstance(excluded_columns, str):  
            try:
                excluded_columns = ast.literal_eval(excluded_columns)  
            except (ValueError, SyntaxError):
                raise ValueError("El parámetro 'excluded_columns' no es una lista válida ni se pudo convertir.")

        if excluded_columns is not None and not isinstance(excluded_columns, (list, tuple)):
            raise ValueError("El parámetro 'excluded_columns' debe ser una lista o una tupla si se proporciona.")

        table_data = {
            "TableCatalogId": catalog_id,
            "DatabaseName": database_name,
            "TableName": table_name,
            "Name": filter_name,
            "RowFilter": {
                "FilterExpression": row_filter,
            },
            "ColumnNames": columns_name,
        }

        if excluded_columns:
            print(f"El parámetro 'excluded_columns' fue incluido con los datos: {excluded_columns}")
            table_data['ColumnWildcard'] = {'ExcludedColumnNames': excluded_columns}

        if version_id:
            print(f"El parámetro 'version_id' fue incluido con el valor: {version_id}")
            table_data['VersionId'] = version_id    
                
        if not row_filter:
            table_data['RowFilter']['AllRowsWildcard'] = {}

        try:
            response = self.lakeformation.create_data_cells_filter(
                TableData=table_data
            )
            print(f"Filtro '{filter_name}' creado exitosamente: {response}")
            return response

        except ClientError as e:
            print(f"Error creando el filtro '{filter_name}': {e}")
            return None



    def grant_permissions_data_filter(self, principal_arn, catalog_id, filter_name, database_name, table_name, permissions, permissions_with_grant_option, action_flag):
        
        try:
            resource = {
                'DataCellsFilter': {
                    'TableCatalogId': catalog_id,
                    'DatabaseName': database_name,
                    'TableName': table_name,
                    'Name': filter_name
                }
            }
            
            if action_flag=='grant':
                response = self.lakeformation.grant_permissions(
                    Principal={'DataLakePrincipalIdentifier': principal_arn},
                    Resource=resource,
                    Permissions=permissions,  
                    PermissionsWithGrantOption=permissions_with_grant_option
                )
                print(f"Filtro '{filter_name}' creado exitosamente: {response}")
            elif action_flag=='revoke':
                response = self.lakeformation.revoke_permissions(
                    Principal={'DataLakePrincipalIdentifier': principal_arn},
                    Resource=resource,
                    Permissions=permissions,  
                    PermissionsWithGrantOption=permissions_with_grant_option
                )
                print(f"Filtro '{filter_name}' revocado exitosamente: {response}")
            
        except Exception as e:
            print(f"Error en la acción {action_flag} del filtro '{filter_name}': {e}")
            return {"statusCode": 500, "error": str(e)}
    
            

if __name__ == '__main__':
    path_file = os.getenv("PATH_FILE")   
    permissions = Permissions(path_file)
    
    data = permissions.read_all_env_files()

    for config in data:
        
        flag = config.get("FLAG_PERMISSIONS")
        print("----------------------------")
        print(f"Processing: {flag}")
        print("----------------------------")    

        print("----------------------------")
        print("Se inicia la ejecución del script")
        print("----------------------------")

        if flag == 'create_lf_tags':
            print("----------------------------")
            print("create_lf_tags")
            print("----------------------------")
            response = permissions.create_lf_tags(
                config.get("TAG_KEY"), 
                config.get("TAG_VALUES")
            )
            
        elif flag == 'assign_lf_tags_columns':
            print("----------------------------")
            print("assign_lf_tags_columns")
            print("----------------------------")
            
            response = permissions.assign_lf_tags_columns(
                config.get("DATABASE_NAME"), 
                config.get("TABLE_NAME"), 
                config.get("CATALOG_ID"), 
                config.get("ASSIGN_TAG"),
                config.get("COLUMN_NAME")
            )
            
        elif flag == 'assign_lf_tags_tb_db':
            print("----------------------------")
            print("assign_lf_tags_tb_db")
            print("----------------------------")
            
            response = permissions.assign_lf_tags(
                config.get("DATABASE_NAME"), 
                config.get("TABLE_NAME"), 
                config.get("CATALOG_ID"), 
                config.get("ASSIGN_TAG")
            )
            
        elif flag == 'grant_permissions':
            print("----------------------------")
            print("grant_permissions")
            print("----------------------------")
            response = permissions.grant_permissions(
                config.get("ROLE_ARN"), 
                config.get("LF_TAGS"), 
                config.get("PERMISSIONS"), 
                config.get("PERMISSIONS_WITH_GRANT_OPTION"), 
                config.get("RESOURCE_TYPE"),
                config.get("CATALOG_ID"),
                config.get("ACTION")
            )
            
        elif flag == 'data_filters':
            print("----------------------------")
            print("data_filters")
            print("----------------------------")
            response = permissions.create_data_cells_filter(
                config.get("CATALOG_ID"),
                config.get("DATABASE_NAME"),
                config.get("TABLE_NAME"),
                config.get("FILTER_NAME"),
                config.get("ROW_FILTER"),
                config.get("COLUMNS_NAME"),
                config.get("EXCLUDED_COLUMNS"),
                config.get("VERSION_ID")
            )
        elif flag == 'grant_permissions_data_filter':
            print("----------------------------")
            print("grant_permissions_data_filter")
            print("----------------------------")
            response = permissions.grant_permissions_data_filter(
                config.get("ROLE_ARN"),
                config.get("CATALOG_ID"),
                config.get("FILTER_NAME"),
                config.get("DATABASE_NAME"),
                config.get("TABLE_NAME"),
                config.get("PERMISSIONS"),
                config.get("PERMISSIONS_WITH_GRANT_OPTION"),
                config.get("ACTION")
            )
        else:
            response = 'Invalid flag_permissions'
            exit(1)

        print("----------------------------")
        print("Se finaliza la ejecución del script")
        print("----------------------------")


