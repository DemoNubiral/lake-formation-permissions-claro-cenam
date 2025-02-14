import boto3
import os
import json



class Permissions:
    def __init__(self, path_dir):
        self.path_dir = path_dir
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
        
    
    def read_file_env(self, file_path):
        
        with open(file_path, 'r') as file:
            env_data_list = [] # Generaria una lista para almacenar multiples configuraciones dentro del archivo .env
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



if __name__ == '__main__':
    # Parámetros de configuración
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
            response = permissions.create_lf_tags(config.get("TAG_KEY"), config.get("TAG_VALUES"))
            print("----------------------------")
            print(f"RESPONSE: {response}")
            print("----------------------------")
        elif flag == 'assign_lf_tags':
            print("----------------------------")
            print("assign_lf_tags")
            print("----------------------------")
            response = permissions.assign_lf_tags(config.get("DATABASE_NAME"), 
                                                config.get("TABLE_NAME"), 
                                                config.get("CATALOG_ID"), 
                                                config.get("ASSIGN_TAG"))
            print("----------------------------")
            print(f"RESPONSE: {response}")
            print("----------------------------")
        elif flag == 'grant_permissions':
            print("----------------------------")
            print("grant_permissions")
            print("----------------------------")
            response = permissions.grant_permissions(config.get("ROLE_ARN"), config.get("LF_TAGS"), 
                                              config.get("PERMISSIONS"), config.get("PERMISSIONS_WITH_GRANT_OPTION"), 
                                              config.get("RESOURCE_TYPE"), config.get("CATALOG_ID"))
            print("----------------------------")
            print(f"RESPONSE: {response}")
            print("----------------------------")
        else:
            response = 'Invalid flag_permissions'
            print("----------------------------")
            print(response)
            print("----------------------------")
            exit(1)

        print("----------------------------")
        print("Se finaliza la ejecución del script")
        print("----------------------------")


