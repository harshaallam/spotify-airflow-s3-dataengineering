from airflow import DAG
from airflow.operators.python import PythonOperator
import boto3
from datetime import datetime, timedelta
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from airflow.models import Variable
from io import StringIO
import pandas as pd
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.dates import days_ago


default_arguments={
    'owner':'Harsha',
    'depends_on_past':False,
    'start_date':days_ago(1)
}

dag=DAG(
    dag_id='spotify_ETL_pipeline',
    description='ETL pipeline using Spotify data',
    default_args=default_arguments,
    schedule_interval=None
)


def _fetch_raw_data(**kwargs):
    client_id=Variable.get('Spotify_client_id')
    client_secret=Variable.get('Spotify_client_secret')
    spotify_url=Variable.get('Spotify_URL')
    aws_key_id=Variable.get('aws_key_id')
    aws_secret_key=Variable.get('aws_secret_key')
    client_creds_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_creds_manager)
    uri=spotify_url.split('/')[-1]
    playlist_tracks=sp.playlist_tracks(uri)
    file_name='spotify_pk_raw_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.json'
    kwargs['ti'].xcom_push(key='Spotify_filename',value =file_name),
    kwargs['ti'].xcom_push(key='Spotify_data',value=json.dumps(playlist_tracks))

def _extract_data_s3(**kwargs):
    bucket_name='spotify-airflow-pipeline'
    s3_hook=S3Hook(aws_conn_id='aws_conn')
    file_names=[]
    spotify_data=[]
    file_keys=s3_hook.list_keys(bucket_name=bucket_name,prefix='raw_data/to_process/')
    for file in file_keys:
        if file.endswith('.json'):
            data=s3_hook.read_key(file,bucket_name)
            spotify_data.append(json.loads(data))
    kwargs['ti'].xcom_push(key='spotify_data',value=spotify_data)

def _process_album_data(**kwargs):
    spotify_data=kwargs['ti'].xcom_pull(task_ids='extract_data_from_s3',key='spotify_data')
    album_list=[]
    for data in spotify_data:
        for cont in data['items']:
            if cont.get('track') is not None:
                print('GETTING INTO ALBUM')
                album_id=cont['track']['album']['id']
                album_name=cont['track']['album']['name']
                release_date=cont['track']['album']['release_date']
                total_tracks=cont['track']['album']['total_tracks']
                uri=cont['track']['album']['uri']
                album_dict={'album_id':album_id,'album_name':album_name,'release_date':release_date,'total_tracks':total_tracks,'uri':uri}
                album_list.append(album_dict)
    album_df=pd.DataFrame.from_dict(album_list)
    album_df.drop_duplicates(subset=['album_id'],inplace=True)
    album_df['release_date']=pd.to_datetime(album_df['release_date'])
    album_buffer=StringIO()
    album_df.to_csv(album_buffer,index=False)
    album_data=album_buffer.getvalue()
    kwargs['ti'].xcom_push(key='album_csv_data',value=album_data)

def _process_song_data(**kwargs):
    spotify_data=kwargs['ti'].xcom_pull(task_ids='extract_data_from_s3',key='spotify_data')
    song_list=[]
    for data in spotify_data:
        for row in data['items']:
            if row.get('track') is not None:
                print('GETTING INTO SONG')
                song_id=row['track']['id']
                song_name=row['track']['name']
                song_duration=row['track']['duration_ms']
                song_popularity=row['track']['popularity']
                song_added=row['added_at']
                song_url=row['track']['external_urls']['spotify']
                album_id=row['track']['album']['id']
                artist_id=row['track']['artists'][0]['id']
                song_dict={'song_id':song_id,'song_name':song_name,'song_duration':song_duration,'song_popularity':song_popularity,'song_added':song_added,'song_url':song_url,'album_id':album_id,'artist_id':artist_id}
                song_list.append(song_dict)        
    song_df=pd.DataFrame.from_dict(song_list)
    song_df.drop_duplicates(subset=['song_id'],inplace=True)
    song_buffer=StringIO()
    song_df.to_csv(song_buffer,index=False)
    song_data=song_buffer.getvalue()
    kwargs['ti'].xcom_push(key='song_csv_data',value=song_data)


def _process_artist_data(**kwargs):
    spotify_data=kwargs['ti'].xcom_pull(task_ids='extract_data_from_s3',key='spotify_data')
    artist_list=[]
    for data in spotify_data:
        for row in data['items']:
            for key,val in row.items():
                if key=='track' and val is not None:
                    print('GETTING INTO ARTIST')
                    for artist in val['artists']:
                        artist_id=artist['id']
                        artist_name=artist['name']
                        artist_href=artist['href']
                        artist_uri=artist['uri']
                        artist_dict={'artist_id':artist_id,'artist_name':artist_name,'artist_href':artist_href,'artist_uri':artist_uri}
                        artist_list.append(artist_dict)
    artist_df=pd.DataFrame.from_dict(artist_list)
    artist_df.drop_duplicates(subset=['artist_id'],inplace=True)
    artist_buffer=StringIO()
    artist_df.to_csv(artist_buffer,index=False)
    artist_data=artist_buffer.getvalue()
    kwargs['ti'].xcom_push(key='artist_csv_data',value=artist_data)

def _move_processed_data(**kwargs):
    #file_names=kwargs['ti'].xcom_pull(task_ids='extract_data_from_s3',key='file_names')
    bucket_name='spotify-airflow-pipeline'
    prefix='raw_data/to_process/'
    target_prefix='raw_data/processed_file/'
    s3_hook=S3Hook(aws_conn_id='aws_conn')
    key_list=s3_hook.list_keys(bucket_name=bucket_name,prefix=prefix)

    for key in key_list:
        if key.endswith('.json'):
            new_key=key.replace(prefix,target_prefix)
            s3_hook.copy_object(
                source_bucket_key=key,
                dest_bucket_key=new_key,
                source_bucket_name=bucket_name,
                dest_bucket_name=bucket_name
            )
            s3_hook.delete_objects(bucket=bucket_name,keys=key)


fetch_raw_data_task=PythonOperator(
    task_id='fetch_raw_spotify_data',
    python_callable=_fetch_raw_data,
    dag=dag
)

store_raw_s3_task=S3CreateObjectOperator(
    task_id="store_raw_file_s3",
    aws_conn_id="aws_conn",
    s3_bucket='spotify-airflow-pipeline',
    s3_key="raw_data/to_process/{{task_instance.xcom_pull(task_ids='fetch_raw_spotify_data',key='Spotify_filename')}}",
    data="{{task_instance.xcom_pull(task_ids='fetch_raw_spotify_data',key='Spotify_data')}}",
    replace=True,
    dag=dag
)

extract_data_s3_task=PythonOperator(
    task_id='extract_data_from_s3',
    python_callable=_extract_data_s3,
    dag=dag
)


process_album_data_task=PythonOperator(
    task_id='process_album_data',
    python_callable=_process_album_data,
    dag=dag
)

store_album_s3_task=S3CreateObjectOperator(
    task_id='store_album_to_s3',
    aws_conn_id='aws_conn',
    s3_bucket='spotify-airflow-pipeline',
    s3_key='transformed/albums/album_pk_transformed_{{ ts_nodash }}.csv',
    data="{{ task_instance.xcom_pull(task_ids='process_album_data',key='album_csv_data') }}",
    replace=True,
    dag=dag
)

process_song_data_task=PythonOperator(
    task_id='process_song_data',
    python_callable=_process_song_data,
    dag=dag
)


store_song_s3_task=S3CreateObjectOperator(
    task_id='store_song_to_s3',
    aws_conn_id='aws_conn',
    s3_bucket='spotify-airflow-pipeline',
    s3_key='transformed/songs/song_pk_transformed_{{ ts_nodash }}.csv',
    data="{{ task_instance.xcom_pull(task_ids='process_song_data',key='song_csv_data') }}",
    replace=True,
    dag=dag
)


process_artist_data_task=PythonOperator(
    task_id='process_artist_data',
    python_callable=_process_artist_data,
    dag=dag
)

store_artist_s3_task=S3CreateObjectOperator(
    task_id='store_artist_to_s3',
    aws_conn_id='aws_conn',
    s3_bucket='spotify-airflow-pipeline',
    s3_key='transformed/artists/artist_pk_transformed_{{ ts_nodash }}.csv',
    data="{{ task_instance.xcom_pull(task_ids='process_artist_data',key='artist_csv_data') }}",
    replace=True,
    dag=dag
)

move_processed_file_task=PythonOperator(
    task_id='move_processed_file',
    python_callable=_move_processed_data,
    provide_context=True,
    dag=dag
)


fetch_raw_data_task >> store_raw_s3_task >> extract_data_s3_task 
extract_data_s3_task >> process_album_data_task >> store_album_s3_task
extract_data_s3_task >> process_song_data_task >> store_song_s3_task
extract_data_s3_task >> process_artist_data_task >> store_artist_s3_task
store_album_s3_task >> move_processed_file_task
store_song_s3_task >> move_processed_file_task
store_artist_s3_task >> move_processed_file_task