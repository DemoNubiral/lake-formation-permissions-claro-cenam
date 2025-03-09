# **Permisos Lake Formation Claro-Cenam**

## **1. Lake Formation Permissions Workflow**

Este repositorio contiene el flujo de trabajo (workflow) para la creación y gestión de permisos en AWS Lake Formation.

### **Descripción**

El workflow se ejecuta automáticamente al realizar cambios en los siguientes directorios:

* `table-permissions/`

* `database-permissions/`

* `create-lf-tags/`

* `assign-lf-tags/`

* `data-filters/`


Cada cambio en estos directorios activará la ejecución del workflow para procesar y aplicar los permisos correspondientes en AWS Lake Formation.

### **Configuración del Workflow**

#### **Eventos de activación**

El workflow se ejecuta en los siguientes casos:

* Push a las ramas main o dev que incluyan cambios en los directorios mencionados.

* Ejecución manual a través de `workflow_dispatch`

#### **Permisos requeridos**

El workflow requiere los siguientes permisos en GitHub Actions:

```yaml
permissions:
  id-token: write
  contents: read
  issues: write
  pull-requests: write
```

#### **Variables de entorno**

El workflow utiliza variables y secretos almacenados en GitHub:

* `AWS_REGION`: Región de AWS donde se aplicarán los permisos.

* `AWS_ROL`: Rol de AWS con permisos para gestionar Lake Formation.

### **Pasos del Workflow**

**1. Checkout del código:** Se obtiene la última versión del código con historial completo.

**2. Configuración de credenciales de AWS:** Se establece la conexión con AWS utilizando GitHub OIDC.

**3. Detección de archivos modificados:** Se identifican los archivos `.env` modificados en los directorios de permisos.

**4. Procesamiento de archivos:** Se copian los archivos modificados a un directorio temporal (`variable_process`).

**5. Configuración de Python**: Se instala Python 3.12.6 y las dependencias necesarias.

**6. Ejecución del script de permisos:** Se ejecuta el script `permissions.py` para aplicar los permisos en AWS Lake Formation.

## **2. Script de permisos**

El script de `permissions.py` se encarga de gestionar los permisos de AWS Lake Formation mediante el uso de etiquetas LF-Tags y filtros de celdas de datos.

### **Explicación del código**

El script está diseñado para conceder o revocar permisos en Lake Formation basado en etiquetas (LF-Tags). Se compone de las siguientes partes:

**1. Inicialización de la clase:**

* Se crea un cliente de `lakeformation` con `boto3`

* Se recibe un parámetro `path_dir` para gestionar los archivos de permisos.

**2. Método:**

* Recibe información sobre el `role_arn`, etiquetas `lf_tags`, permisos y el tipo de recurso.

* Construye el diccionario `resource` que define la política de etiquetas en Lake Formation.

* Dependiendo del `action_flag`:

    * Si es `grant`, otorga permisos mediante `grant_permissions`.

    * Si es `revoke`, revoca los permisos con `revoke_permissions`.

* Muestra un mensaje de éxito o devuelve un error si ocurre alguna excepción.

**3. Ejecución del script:**

* Obtiene la variable de entorno `PATH_FILE`.

* Crea una instancia de la clase `permissions`.

* Muestra mensajes indicando el inicio y la finalización de la ejecución.

## **3. Directorios y archivos `.env`**

Cada una de las siguientes carpetas contiene archivos `.env` con variables necesarias para definir permisos en Lake Formation:

* `create-lf-tags/:` Contiene archivos `.env` que definen las etiquetas LF-Tags a ser creadas en Lake Formation. Las variables asignadas dentro del archivo `.env` son: TAG_KEY (tipo dato: string), TAG_VALUES (tipo dato: lista), FLAG_PERMISSIONS (tipo dato: string).

* `assign-lf-tags/:` Define qué etiquetas deben asignarse a qué recursos en AWS Lake Formation. Las variables asignadas dentro del archivo `.env` son: CATALOG_ID (tipo dato: string), DATABASE_NAME (tipo dato: string), TABLE_NAME (tipo dato: string), TAG_KEY (tipo dato: string), TAG_VALUES (tipo dato: lista), FLAG_PERMISSIONS (tipo dato: string).

* `data-filters/:` Contiene configuraciones de filtros de datos para restringir el acceso a nivel de filas o columnas en las tablas de Glue Data Catalog. Las variables asignadas dentro del archivo `.env` son: CATALOG_ID (tipo dato: string), DATABASE_NAME (tipo dato: string), TABLE_NAME (tipo dato: string), FILTER_NAME (tipo dato: string), ROW_FILTER (tipo dato: string), COLUMNS_NAME (tipo dato: lista), EXCLUDED_COLUMNS (tipo dato: lista), VERSION_ID (tipo dato: None), FLAG_PERMISSIONS (tipo dato: string).  

* `database-permissions/:` Especifica los permisos a nivel de bases de datos en AWS Lake Formation.

* `table-permissions/:` Define los permisos específicos para tablas dentro de las bases de datos registradas en AWS Lake Formation. Las variables asignadas dentro del archivo `.env` son: ROLE_ARN (tipo dato: string), CATALOG_ID (tipo dato: string), LF_TAGS (tipo dato: lista de diccionarios), PERMISSIONS (tipo dato: string), PERMISSIONS_WITH_GRANT_OPTION (tipo dato: string), RESOURCE_TYPE (tipo dato: string), FLAG_PERMISSIONS (tipo dato: string).

