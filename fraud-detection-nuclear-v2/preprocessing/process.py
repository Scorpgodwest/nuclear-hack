import pandas as pd
import numpy as np
def process(df: pd.DataFrame, terminal: pd.DataFrame, cstm: pd.DataFrame):
    cstm.columns = [x.lower() for x in cstm.columns]
    terminal.columns = [x.lower() for x in terminal.columns]
    df.columns = [x.lower() for x in df.columns]
    df = pd.merge(df, terminal, on='terminal_id', how='left')
    df = pd.merge(df, cstm, on='customer_id', how='left')


    
    df = df.drop_duplicates()
    df['tx_datetime'] = pd.to_datetime(df['tx_datetime'])
    df['amount_above_mean'] = df['tx_amount'] - df['mean_amount'] 
    df['deviation_std'] = round((df['tx_amount'] - df['mean_amount']) / df['std_amount'], 5)
    df['deviation_big'] = (df['deviation_std'] > 1.96).astype(int)
    df['hour'] = df['tx_datetime'].dt.hour
    df['day_of_week'] = df['tx_datetime'].dt.day_of_week
    df['log_amount'] = round(np.log(df['tx_amount']),5)

    df = df.sort_values(by=['customer_id', 'tx_datetime'])
    df['minutes_since_last_tx'] = df.groupby('customer_id')['tx_datetime'].diff().apply(lambda x: x.seconds/60).round()
    df['minutes_since_last_tx'] = df['minutes_since_last_tx'].fillna(method='bfill')

    allLat  = list(df['y_customer_id']) + list(df['y_terminal_id'])
    medianLat  = sorted(allLat)[int(len(allLat)/2)]
    latMultiplier  = 111.32

    df['y_customer_id']   = latMultiplier  * (df['y_customer_id']   - medianLat)
    df['y_terminal_id']   = latMultiplier  * (df['y_terminal_id']  - medianLat)
    allLong = list(df['x_customer_id']) + list(df['x_terminal_id'])

    medianLong  = sorted(allLong)[int(len(allLong)/2)]

    longMultiplier = np.cos(medianLat*(np.pi/180.0)) * 111.32

    df['x_customer_id']  = longMultiplier * (df['x_customer_id']  - medianLong)
    df['x_terminal_id']  = longMultiplier * (df['x_terminal_id'] - medianLong)

    df['long_diff'] = df['x_terminal_id'] - df['x_customer_id']
    df['lat_diff'] = df['y_terminal_id'] - df['y_customer_id']

    df['distance_km'] = round((df['long_diff']**2 + df['lat_diff']**2)**(1/2), 2)

    df = df.drop(['long_diff', 'lat_diff'], axis=1)
    df = df.drop(['transaction_id', 'terminal_id', 'terminal_id', 'tx_datetime', 'customer_id','x_terminal_id', 'y_terminal_id', 'x_customer_id', 'y_customer_id', 'available_terminals'], axis=1)
    df = df.round({'mean_amount':5,'std_amount':5,'mean_nb_tx_per_day':5,'amount_above_mean':5})
    float64_cols = list(df.select_dtypes(include='float64'))
    df[float64_cols] = df[float64_cols].astype('float32')
    return df