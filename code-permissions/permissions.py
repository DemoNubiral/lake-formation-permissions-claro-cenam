import boto3
import os
import json
# Inicializa el cliente de Lake Formation
lakeformation = boto3.client('lakeformation')


# Parámetros de configuración
role_arn = os.getenv['ROLE_ARN']
lf_tags = os.getenv['LF_TAGS']
permissions = os.getenv["PERMISSIONS"]
permissions_with_grant_option = os.getenv["PERMISSIONS_WITH_GRANT_OPTION"]

try:
    # Crea la política para otorgar permisos utilizando LF-tags
    response = lakeformation.grant_permissions(
        Principal={
            'DataLakePrincipalIdentifier': role_arn
        },
        Resource={
            'LFTagPolicy': {
                'CatalogId': '123456789012',  # Reemplaza con tu ID de cuenta de AWS
                'ResourceType': os.getenv["RESOURCE_TYPE"],     # Cambia a 'DATABASE' si deseas aplicar a bases de datos
                'Expression': json.loads(lf_tags)
            }
        },
        Permissions=permissions,
        PermissionsWithGrantOption=permissions_with_grant_option
    )

    print("Permisos otorgados exitosamente:", response)

except Exception as e:
    print("Ocurrió un error al asignar los permisos:", str(e))
