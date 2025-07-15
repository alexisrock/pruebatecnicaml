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
- se filtraon los datos que correspondieras a una semana antes de la fecha y no maximo 3 segun las indicaciones
- se ordenaron los datos por fecha


### 3 Calculo de metricas por impresion

se calcularon las metricas, exclusivamente para el mismo 'user_id' y 'value_prop'

- has_click: muestra un booleano indicando si hubo click el mismo dia.
- user_print_result: cantidad de veces que el usuario vio la misma value_prop en las ultimas 3 semanas anteriores
- user_taps_result: cantidad de click que el usuario hizo en las ultimas 3 semanas
- user_payments_result: cantidad de pagos realizados asociados a el value_prop
- amounts_spent_result: suma de los importes gastados en esa value_prop

### 4 Manejo de fechas

- se calculo la fecha para 3 semanas atras.
- si la fecha evaluada caia antes del primer registro disponible en los datasets se ajustaba para respetar el rango minimo

### Output generado

- El resultado es un DataFrame consolidado con las 8 columnas