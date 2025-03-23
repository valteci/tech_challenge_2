# Tech Challenge - Pipeline Batch Bovespa

Este projeto consiste na construção de um pipeline de dados completo para extrair, processar e analisar dados do pregão da B3 utilizando AWS S3, Glue, Lambda e Athena.

## Configuração do Ambiente

### 1. Criar Ambiente Virtual
Para garantir que todas as dependências sejam instaladas corretamente, é altamente recomendável criar um ambiente virtual.

No Linux ou MacOS:
```bash
python3 -m venv .venv
source .venv/bin/activate
```

No Windows:
```powershell
python3 -m venv .venv
.\.venv\Scripts\activate
```

### 2. Instalar Dependências
Após ativar o ambiente virtual, instale as dependências necessárias executando:
```bash
pip install -r requirements.txt
```

## Configuração das Credenciais AWS

Para permitir que o script envie dados para o Amazon S3 e inicie o job Glue, é necessário configurar suas credenciais da AWS no arquivo `~/.aws/credentials`:

**Exemplo de configuração:**
```
[default]
aws_access_key_id = SUA_CHAVE_DE_ACESSO
aws_secret_access_key = SUA_CHAVE_SECRETA
region = us-east-1  # Ajuste conforme necessário
```

## Executando o Pipeline

Para iniciar o pipeline, utilize o seguinte comando:
```bash
python main.py
```

## Fluxo do Pipeline

1. **Scrap de dados**: O script realizará web scraping no site da B3 para obter os dados do pregão.
2. **Criação do DataFrame**: Os dados serão organizados em um DataFrame usando a biblioteca pandas.
3. **Geração do arquivo Parquet**: Esse arquivo será gerado a partir do DataFrame e salvo no bucket S3 com partição diária.
4. **Ativação da Lambda**: O upload do Parquet aciona uma função Lambda que por sua vez inicia um job no AWS Glue.
5. **Job Glue**: O job realizará as seguintes etapas:
   - **Agrupamento numérico e sumarização.**
   - **Renomeação de duas colunas existentes.**
   - **Cálculo envolvendo campos de data.**
6. **Dados Refinados**: O resultado do job Glue será salvo no bucket S3 na pasta `refined/`, particionado por data e pelo nome/abreviação da ação do pregão.
7. **Catálogo Glue**: O Glue catalogará automaticamente os dados e criará uma tabela no banco de dados `default` do Glue Catalog.
8. **Acesso via Athena**: Os dados serão consultáveis diretamente no Amazon Athena.

## Requisitos Adicionais

- Certifique-se de que seu usuário tenha permissões suficientes para criar e manipular recursos no AWS S3, Glue, Lambda e Athena.