import pandas as pd
from datetime import timedelta




def extraer_datos_json(ruta):
    try:
        if ruta is None or not isinstance(ruta, str):
            raise ValueError("La ruta debe ser una cadena de texto válida.")
       

        df = pd.read_json(ruta, lines=True)        
        df['value_prop'] = df['event_data'].apply(lambda x: x['value_prop'])
        df['date'] = pd.to_datetime(df['day'], errors='coerce')
        df= df[['date', 'user_id', 'value_prop']].copy()
        df=df.sort_values(by='date').reset_index(drop=True)
        return df
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        return None

def extraer_datos_csv(ruta):
    try:
        if ruta is None or not isinstance(ruta, str):
            raise ValueError("La ruta debe ser una cadena de texto válida.")
        
        df = pd.read_csv(ruta)
        df['date'] = pd.to_datetime(df['pay_date'], errors='coerce')
        df['total'] = pd.to_numeric(df['total'], errors='coerce')
        df['total'] = df['total'].astype(float)
        df = df[['date', 'total', 'user_id', 'value_prop']].copy()
        df = df.sort_values(by='date').reset_index(drop=True)
        return df
    except Exception as e:
        print(f"Error al extraer datos: {e}")
        return None 

def filtrar_semanas(df):
    try:
        max_date_prints = df['date'].max()
        start_last_week = max_date_prints - timedelta(days=6) 
        df = df[df['date'] <= start_last_week]

        date = df['date'].max()
        end_date = date - timedelta(days=1)
        start_date = end_date - timedelta(days=20)
        df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]  

        return df

    except Exception as e:
        print(f"error en el proceso de filtrar semanas: {e}")



def iterar_datos(df_prints, df_taps, df_payments):

    try:
        user_print_result = df_prints.groupby(['user_id','value_prop'])['value_prop'].size().reset_index(name='count_print')
        user_taps_result = df_taps.groupby(['user_id','value_prop'])['value_prop'].size().reset_index(name='count_taps')
        user_payments_result = df_payments.groupby(['user_id','value_prop'])['value_prop'].size().reset_index(name='count_payments')
        amounts_spent_result = df_payments.groupby(['user_id','value_prop'])['total'].sum().reset_index(name='total_payments')

        merge_print_taps = pd.merge(
            user_print_result,
            user_taps_result,
            on=['user_id', 'value_prop'],
            how='left'
        )
 
        merge_print_taps['count_taps'] = merge_print_taps['count_taps'].fillna(0).astype(int)
        merge_print_taps['has_click'] = merge_print_taps['count_taps'] > 0
        merge_merge_print_taps_payments =pd.merge(
            merge_print_taps,
            user_payments_result,
            on=['user_id', 'value_prop'],
            how='left'
        )

        merge_merge_print_taps_payments['count_payments'] = merge_merge_print_taps_payments['count_payments'].fillna(0).astype(int)

        merge_amounts_spent_result =pd.merge(
            merge_merge_print_taps_payments,
            amounts_spent_result,
            on=['user_id', 'value_prop'],
            how='left'
        )

        merge_amounts_spent_result['total_payments'] = merge_amounts_spent_result['total_payments'].fillna(0).astype(float)


        if len(merge_amounts_spent_result)  == 0:
            raise ValueError("no existen datos para crear un dataframe")
        
        
        merge_amounts_spent_result=merge_amounts_spent_result[['user_id', 'value_prop','has_click','count_print','count_taps','count_payments','total_payments' ]]
        return merge_amounts_spent_result  

            
 
    except Exception as e:
        print(f"Error al iterar datos: {e}")





def main():
    try:

        df_prints = extraer_datos_json('./inputs/prints.json')
        df_taps= extraer_datos_json('./inputs/taps.json') 
        df_payments = extraer_datos_csv('./inputs/pays.csv')

        if df_prints.empty or df_taps.empty or df_payments.empty or df_prints is None or df_taps is None or df_payments is None:
            raise Exception("No se encontraron datos en los archivos de entrada.")
            
             
        df_filtraddo_print = filtrar_semanas(df_prints)
        df_filtrado_taps = filtrar_semanas(df_taps)
        df_filtrado_payments = filtrar_semanas(df_payments)

        if df_filtraddo_print.empty or df_filtrado_taps.empty or df_filtrado_payments.empty or df_filtraddo_print is None or df_filtrado_taps is None or df_filtrado_payments is None:
            raise Exception("No se encontraron dataframe filtrados.")
            
        df_final = iterar_datos(df_filtraddo_print, df_filtrado_taps, df_filtrado_payments)  
    
        if df_final.empty or df_final is None:
            raise Exception("No se generaron datos en el DataFrame final.")

        print(df_final.head(10)) 

    except Exception as e:
        print(f"error en el procesamiento: {e}")

if __name__ == "__main__":
    main()