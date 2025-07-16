# Decisiones Técnicas - Ejercicio Data Engineer Mercado Pago

## Objetivo del pipelie

El objetivo fue construir un pipeline en python que consolide informacion de tres fuentes distintas
('prints', 'taps', 'payments') y genere un dataset final que permita entrenar un modelo de Machine Learning
para predecir el orden de las propuestas de valor

## Fuentes de datos utilizadas

- prints.json: historial de impresiones
- taps.json historial de clics sobre propuestas de valor
- pays.csv historial de pagos asociado a la propuesta de valor


## Decisiones de implementacion

### 1. Lectura de archivos
tanto para los arcivos que estaban en formato json como csv se utilizo la libreria de pandas

-  para los archivos que estaban en formato json se utilizo el metodo read_json(.., lines= true) dado que el formato de los documentos tipo json son jsonlines.

- para los archivos csv se utilizo el metodo read_csv, asegurando el parseo correcto de las fechas y las cantidades

### Validacion y limpieza de datos

- se descartaron las fechas no parseables
- se filtraon los datos y se seleccionan los datos de una semana antes
- luego se filtran los datos respecto a esa semana para 3 semanas atras

### 3 Calculo de metricas por impresion

se calcularon las metricas, exclusivamente para el mismo 'user_id' y 'value_prop' todas las agrupaciones se hicieron por user_id y value_prop

- has_click: muestra un booleano indicando si hubo click el mismo.
- user_print_result: cantidad de veces que el usuario vio la misma value_prop en las ultimas 3 semanas anteriores
- user_taps_result: cantidad de click que el usuario hizo en las ultimas 3 semanas anteriores
- user_payments_result: cantidad de pagos realizados asociados a el value_prop en las ultimas 3 semanas anteriores
- amounts_spent_result: suma de los importes gastados en esa value_prop

### 4 Unir los Resultados

se escoge hacer un merge left para que me muestre los taps y los payments sin  importar si  los campos count_payments,  count_taps, total_payments, son nulos

- el primer merge que se hace es entre user_print_result y user_taps_result de manera que  para el campo count_taps
se coloca en nulo por 0

- el resultado de lo anterior se une con df_payments de manera que  para el campo count_payments
se coloca en nulo por 0

- luego el resultado de lo anterior se une con amounts_spent_result el cual es la suma de los pagos realizados y para el campo total_payments se coloca con cero



 
### Output generado

- El resultado es un DataFrame consolidado con las 8 columnas