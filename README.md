# DS_PROY1
Proyecto 1 de data science

En este proyecto se tuvo el objetivo de obtener datos de instituciones de diversificado en Guatemala por medio de la página del Mineduc. La información fue otbtenida desde el siguiente enlace: https://www.mineduc.gob.gt/BUSCAESTABLECIMIENTO_GE/. La parte complicada del procesamiento de datos comienza con la obtención de los mismos datos. En la página todos las instituciones están listadas y son una descarga separada por lo que opara obtener los datos se hizo el siguiente plane:

* Utilizar web scrapping para descargar de forma automática la información de los institutos.
* Hacer un script para procesar los archivos con formato irregular y juntarlos en un csv
* Examinar los datos y eliminar registros con muchas datos faltantes
* Manejar datos vacíos en el set de datos
* Verificar valores repetidos y normalizar las columnas con valores problemáticos o múltiples

Con estos pasos al final se obtuvieron datos completos, sin información vacía y lista para procesamiento. El csv que fue el resultado del webscarpping se llama establecimientos. El csv resultante se llama establecimientos_limpios.csv