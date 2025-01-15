import boto3
import os
import json
# Inicializa el cliente de Lake Formation
lakeformation = boto3.client('lakeformation')


# Parámetros de configuración
role_arn = os.getenv('ROLE_ARN')
lf_tags_str = os.getenv('LF_TAGS')
permissions_str = os.getenv("PERMISSIONS")
permissions_with_grant_option_str = os.getenv("PERMISSIONS_WITH_GRANT_OPTION")
resource_type = os.getenv('RESOURCE_TYPE')

lf_tags = json.loads(lf_tags_str)
permissions = json.loads(permissions_str)
permissions_with_grant_option = json.loads(permissions_with_grant_option_str)
try:
    # Crea la política para otorgar permisos utilizando LF-tags
    response = lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': role_arn
        },
        Resource={
            'LFTagPolicy': {
                'CatalogId': '015319782619',  # Reemplaza con tu ID de cuenta de AWS
                'ResourceType': resource_type ,     # Cambia a 'DATABASE' si deseas aplicar a bases de datos
                'Expression': lf_tags
            }
        },
        Permissions=permissions,
        PermissionsWithGrantOption=permissions_with_grant_option
    )

    print("Permisos otorgados exitosamente:", response)

except Exception as e:
    print("Ocurrió un error al asignar los permisos:", str(e))
