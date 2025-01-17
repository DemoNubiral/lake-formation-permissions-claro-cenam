import boto3
import os
import json



class Permissions:
    def __init__(self, path_file):
        self.path_file = path_file
        self.lakeformation = boto3.client('lakeformation')

    def grant_permissions(self, role_arn, lf_tags, permissions, permissions_with_grant_option, resource_type, catalog_id):
        try:
            # Crea la política para otorgar permisos utilizando LF-tags
            response = self.lakeformation.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': role_arn
                },
                Resource={
                    'LFTagPolicy': {
                        'CatalogId': catalog_id,  
                        'ResourceType': resource_type ,     # Cambia a 'DATABASE' si deseas aplicar a bases de datos
                        'Expression': lf_tags
                    }
                },
                Permissions=permissions,
                PermissionsWithGrantOption=permissions_with_grant_option
            )

            return response

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
        

    def assign_lf_tags(self, database_name, table_name, catalog_id, tag_key, tag_values):
        try:
            # Asigna etiquetas LF a recursos
            response = self.lakeformation.add_lf_tags_to_resource(
                CatalogId=catalog_id,  
                
                Resource={
                    'Database': {
                        'Name': database_name,
                        'CatalogId': catalog_id,  
                    },
                    'Table': {
                        'DatabaseName': database_name,
                        'Name': table_name,
                        'CatalogId': catalog_id,  
                    },
                    'LFTag': {
                        'CatalogId': catalog_id,  
                        'TagKey': tag_key,
                        'TagValues': tag_values
                    }
                }
            )

            return response
        except Exception as e:
            return str(e)
        
    
    def read_file_env(self):
        file = open(self.path_file, 'r')
        env_data = {}
        for line in file:
            if not line.strip() or line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip("'").strip('"')
            env_data[key] = value
        return env_data


if __name__ == '__main__':
    # Parámetros de configuración
    path_file = os.getenv("PATH_FILE")   
    permissions = Permissions(path_file)
    
    data = permissions.read_file_env()
    json_data_role_arn= data['ROLE_ARN'] if 'ROLE_ARN' in data else ""
    json_data_lf_tags = json.loads(data['LF_TAGS']) if 'LF_TAGS' in data else ""
    json_data_permissions =  json.loads(data['PERMISSIONS']) if 'PERMISSIONS' in data else ""
    json_data_permissions_with_grant_option = json.loads(data['PERMISSIONS_WITH_GRANT_OPTION']) if 'PERMISSIONS_WITH_GRANT_OPTION' in data else ""
    json_data_resource_type = data['RESOURCE_TYPE'] if 'RESOURCE_TYPE' in data else ""
    json_data_flag_permissions = data['FLAG_PERMISSIONS'] if 'FLAG_PERMISSIONS' in data else ""
    json_data_catalog_id = data['CATALOG_ID'] if 'CATALOG_ID' in data else ""
    json_data_tag_key = data['TAG_KEY'] if 'TAG_KEY' in data else ""
    json_data_tag_values = json.loads(data['TAG_VALUES']) if 'TAG_VALUES' in data else ""
    json_data_database_name = data['DATABASE_NAME'] if 'DATABASE_NAME' in data else ""
    json_data_table_name = data['TABLE_NAME'] if 'TABLE_NAME' in data else ""


    print("----------------------------")
    print("Se inicia la ejecución del script")
    print("----------------------------")
    if json_data_flag_permissions == 'create_lf_tags':
        print("----------------------------")
        print("create_lf_tags")
        print("----------------------------")
        response = permissions.create_lf_tags(json_data_tag_key, json_data_tag_values)
    elif json_data_flag_permissions == 'assign_lf_tags':
        print("----------------------------")
        print("assign_lf_tags")
        print("----------------------------")
        response = permissions.assign_lf_tags(json_data_database_name, 
                                              json_data_table_name, 
                                              json_data_catalog_id, 
                                              json_data_tag_key, 
                                              json_data_tag_values)
    elif json_data_flag_permissions == 'grant_permissions':
        print("----------------------------")
        print("grant_permissions")
        print("----------------------------")
        response = permissions.grant_permissions(json_data_role_arn, 
                                                 json_data_lf_tags, 
                                                 json_data_permissions, 
                                                 json_data_permissions_with_grant_option, 
                                                 json_data_resource_type, 
                                                 json_data_catalog_id)
    else:
        response = 'Invalid flag_permissions'
        print("----------------------------")
        print(response)
        print("----------------------------")
        exit(1)

    print("----------------------------")
    print("Se finaliza la ejecución del script")
    print("----------------------------")


