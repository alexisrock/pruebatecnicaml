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


def filtrar_datos_ultima_semana(df):
    try:

        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("El DataFrame debe ser válido.")
        
        fecha_maxima = df['date'].max()
        fecha_inicio = fecha_maxima - pd.Timedelta(days=6)
        
        df_filtrado = df[df['date']  <= fecha_inicio].copy()
        return df_filtrado
    except Exception as e:
        print(f"Error al filtrar datos: {e}")
        return None
    
def iterar_datos(df_prints, df_taps, df_payments):
    result_data =[]    
    try:

        for index, row in df_prints.iterrows():
            user_id = row['user_id']
            date = pd.to_datetime(row['date'].strftime('%Y-%m-%d'))
            value_prop = row['value_prop']
            
            has_click = not df_taps[
                (df_taps['user_id'] == user_id) &
                (df_taps['value_prop'] == value_prop) &
                (df_taps['date'] == date)
            ].empty

            end_date = date - timedelta(days=1)
            start_date = end_date - timedelta(days=20)

            min_date = min(df_prints['date'].min(), df_taps['date'].min(), df_payments['date'].min())

            if start_date < min_date:
                start_date = min_date

                user_prints = df_prints[(df_prints['user_id'] == user_id) &
                                        (df_prints['date'] >= start_date) &
                                        (df_prints['date'] <= end_date)]
                
                user_taps = df_taps[(df_taps['user_id'] == user_id) &
                                    (df_taps['date'] >= start_date) &
                                    (df_taps['date'] <= end_date)]
                
                user_payments = df_payments[(df_payments['user_id']== user_id) &
                                            (df_taps['date'] >= start_date) &
                                            (df_taps['date'] <= end_date)]
                

                user_print_result = user_prints['value_prop'].value_counts().get(value_prop, 0)
                user_taps_result = user_taps['value_prop'].value_counts().get(value_prop, 0)
                user_payments_result = user_payments['value_prop'].value_counts().get(value_prop, 0)
                amounts_spent_result = user_payments[user_payments['value_prop'] == value_prop]['total'].sum()
            
                result_data.append({
                    'user_id': user_id,
                    'date': date.strftime('%Y-%m-%d'),
                    'value_prop': value_prop,
                    'has_click': has_click,
                    'prints_count': user_print_result,
                    'clicks_count': user_taps_result,
                    'payments_count': user_payments_result,
                    'amounts_spent_': amounts_spent_result
                })
            
        return pd.DataFrame(result_data)
    except Exception as e:
        print(f"Error al iterar datos: {e}")





def main():
    try:

        df_prints = extraer_datos_json('./inputs/prints.json')
        df_taps= extraer_datos_json('./inputs/taps.json') 
        df_payments = extraer_datos_csv('./inputs/pays.csv')

        if df_prints.empty or df_taps.empty or df_payments.empty or df_prints is None or df_taps is None or df_payments is None:
            raise Exception("No se encontraron datos en los archivos de entrada.")
            
        
        df_filtrado = filtrar_datos_ultima_semana(df_prints)
        if df_filtrado.empty or df_filtrado is None:
            raise Exception("No se encontraron datos en la última semana en el DataFrame de impresiones.")
        
        df_final = iterar_datos(df_filtrado, df_taps, df_payments)  
    
        if df_final.empty or df_final is None:
            raise Exception("No se generaron datos en el DataFrame final.")

        print(df_final.head(10)) 

    except Exception as e:
        print(f"error en el procesamiento: {e}")

if __name__ == "__main__":
    main()