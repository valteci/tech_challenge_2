import boto3
from botocore.exceptions import NoCredentialsError

s3_client = boto3.client('s3')


def upload(
        your_file_path: str,
        bucket_path: str,
        object_name: str = None,
        prefix: str = None
) -> None:
    """
    Faz o upload de um arquivo local para um bucket no Amazon S3.

    Esta função utiliza o cliente S3 para enviar um arquivo especificado para 
    o bucket indicado, com suporte a nomes personalizados e prefixos opcionais.

    Parâmetros:
    -----------
    your_file_path : str
        Caminho local do arquivo que será enviado para o S3.
    bucket_path : str
        Nome do bucket no qual o arquivo será armazenado.
    object_name : str, opcional
        Nome alternativo para o arquivo no bucket S3. Se não for especificado,
        o nome do arquivo será extraído automaticamente do caminho do arquivo.
    prefix : str, opcional
        Prefixo opcional para organizar o arquivo dentro de uma pasta virtual
        no bucket.

    Comportamento:
    --------------
    - Se `object_name` não for informado, o nome do arquivo será extraído a partir de `your_file_path`.
    - Se `prefix` for informado, ele será adicionado antes do nome do arquivo para criar uma estrutura
      de diretórios virtual no bucket.

    Tratamento de Erros:
    --------------------
    - Caso ocorra um erro durante o upload, uma mensagem de erro será exibida
      no console e a exceção será capturada.

    Exemplo de uso:
    ---------------
    Enviar um arquivo sem prefixo:
        upload('dados/dataset.parquet', 'meu-bucket')
    
    Enviar um arquivo com prefixo (organizando em pastas virtuais no S3):
        upload('dados/dataset.parquet', 'meu-bucket', prefix='raw_data/2025')

    Enviar um arquivo com nome personalizado:
        upload('dados/dataset.parquet', 'meu-bucket', object_name='novo_nome.parquet')
    """
    try:
        if object_name is None:
            object_name = your_file_path.split('/')[-1]
        if prefix:
            object_name = f'{prefix}/{object_name}'

        s3_client.upload_file(your_file_path, bucket_path, object_name)       
        print('\n\033[32mDados enviados para o S3 com sucesso!\033[0m')
    
    except Exception as e:
        print(e)
