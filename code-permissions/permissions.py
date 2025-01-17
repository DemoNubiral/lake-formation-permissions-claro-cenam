import boto3
import os
import json
from dotenv import load_dotenv



class Permissions:
    def __init__(self, role_arn, lf_tags, permissions, permissions_with_grant_option, resource_type, tag_key, tag_values, catalog_id, database_name, table_name):
        self.role_arn = role_arn
        self.lf_tags = lf_tags
        self.permissions = permissions
        self.permissions_with_grant_option = permissions_with_grant_option
        self.resource_type = resource_type
        self.tag_key = tag_key
        self.tag_values = tag_values
        self.catalog_id = catalog_id
        self.database_name = database_name
        self.table_name = table_name
        self.lakeformation = boto3.client('lakeformation')

    def grant_permissions(self):
        try:
            # Crea la política para otorgar permisos utilizando LF-tags
            response = self.lakeformation.grant_permissions(
                Principal={
                    'DataLakePrincipalIdentifier': self.role_arn
                },
                Resource={
                    'LFTagPolicy': {
                        'CatalogId': self.catalog_id,  
                        'ResourceType': self.resource_type ,     # Cambia a 'DATABASE' si deseas aplicar a bases de datos
                        'Expression': self.lf_tags
                    }
                },
                Permissions=self.permissions,
                PermissionsWithGrantOption=self.permissions_with_grant_option
            )

            return response

        except Exception as e:
            return str(e)
        
    
    def create_lf_tags(self):
        try:
            # Crea una etiqueta LF
            response = self.lakeformation.create_lf_tag(
                TagKey=self.tag_key,
                TagValues=self.tag_values
            )

            return response

        except Exception as e:
            return str(e)
        

    def assign_lf_tags(self):
        try:
            # Asigna etiquetas LF a recursos
            response = self.lakeformation.add_lf_tags_to_resource(
                CatalogId=self.catalog_id,  
                
                Resource={
                    'Database': {
                        'Name': self.database_name,
                        'CatalogId': self.catalog_id,  
                    },
                    'Table': {
                        'DatabaseName': self.database_name,
                        'Name': self.table_name,
                        'CatalogId': self.catalog_id,  
                    },
                    'LFTag': {
                        'CatalogId': self.catalog_id,  
                        'TagKey': self.tag_key,
                        'TagValues': self.tag_values
                    }
                }
            )

            return response
        except Exception as e:
            return str(e)


if __name__ == '__main__':
    path_file = os.getenv("PATH_FILES")
    print(path_file)
    data = open(path_file, 'r')
    read_data = data.read()
    print(read_data)
    print(type(read_data))
    print(read_data.split("="))

    # Parámetros de configuración
    role_arn = os.getenv("ROLE_ARN")
    lf_tags_str = os.getenv('LF_TAGS')
    permissions_str = os.getenv('PERMISSIONS')
    permissions_with_grant_option_str = os.getenv('PERMISSIONS_WITH_GRANT_OPTION')
    resource_type = os.getenv("RESOURCE_TYPE")
    tag_key = os.getenv("TAG_KEY")
    tag_values_str = os.getenv("TAG_VALUES")
    catalog_id = os.getenv("CATALOG_ID")
    database_name = os.getenv("DATABASE_NAME")
    table_name = os.getenv("TABLE_NAME")

    flag_permissions = os.getenv("FLAG_PERMISSIONS")

    print(type(flag_permissions))
    print(type(lf_tags_str))
    print(lf_tags_str)
    print(type(permissions_str))
    print(type(permissions_with_grant_option_str))
    print(type(tag_values_str))


    # Transforma los valores de las variables de entorno a listas
    lf_tags = "" if lf_tags_str == None else json.loads(lf_tags_str)
    permissions = "" if permissions_str == None else json.loads(permissions_str)
    permissions_with_grant_option = "" if permissions_with_grant_option_str == None else json.loads(permissions_with_grant_option_str)
    tag_values = "" if tag_values_str == None else json.loads(tag_values_str)

    permissions = Permissions(role_arn, 
                              lf_tags, 
                              permissions, 
                              permissions_with_grant_option, 
                              resource_type, 
                              tag_key, 
                              tag_values,
                              catalog_id,
                              database_name,
                              table_name)
    print("----------------------------")
    print("Se inicia la ejecución del script")
    print("----------------------------")
    if flag_permissions == 'create_lf_tags':
        print("----------------------------")
        print("create_lf_tags")
        print("----------------------------")
        response = permissions.create_lf_tags()
    elif flag_permissions == 'assign_lf_tags':
        print("----------------------------")
        print("assign_lf_tags")
        print("----------------------------")
        response = permissions.assign_lf_tags()
    elif flag_permissions == 'grant_permissions':
        print("----------------------------")
        print("grant_permissions")
        print("----------------------------")
        response = permissions.grant_permissions()
    else:
        response = 'Invalid flag_permissions'
        print("----------------------------")
        print(response)
        print("----------------------------")
        exit(1)

    print("----------------------------")
    print("Se finaliza la ejecución del script")
    print("----------------------------")


