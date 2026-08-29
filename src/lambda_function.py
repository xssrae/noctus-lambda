import os
import json
import base64
import csv
import boto3
from datetime import datetime

# Instanciar o cliente S3 (boto3 está disponível por padrão no runtime do Lambda)
s3_client = boto3.client("s3")

# Dicionário de cache na memória para evitar ler o S3 a cada invocação se o Lambda estiver quente (Warm Start)
CACHE_CLIENTES = {}
CACHE_LAST_LOADED = None

def carregar_clientes_do_s3(bucket_name, key="clientes.csv"):
    """
    Carrega o arquivo de cadastro de clientes do S3 e monta um dicionário para joins rápidos.
    Utiliza cache em memória do Lambda para otimizar tempo e custo de requisições S3 GET.
    """
    global CACHE_CLIENTES, CACHE_LAST_LOADED
    
    # Se o cache já estiver carregado há menos de 10 minutos, reutiliza
    agora = datetime.utcnow()
    if CACHE_CLIENTES and CACHE_LAST_LOADED and (agora - CACHE_LAST_LOADED).seconds < 600:
        print("Reutilizando cadastro de clientes do cache em memoria (Warm Start).")
        return CACHE_CLIENTES

    try:
        print(f"Baixando cadastro de clientes de s3://{bucket_name}/{key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        conteudo_csv = response["Body"].read().decode("utf-8").splitlines()
        
        leitor = csv.DictReader(conteudo_csv)
        clientes_dict = {}
        for linha in leitor:
            cliente_id = int(linha["cliente_id"])
            clientes_dict[cliente_id] = {
                "nome": linha["nome"],
                "data_nascimento": linha["data_nascimento"],
                "email": linha.get("email", "")
            }
        
        CACHE_CLIENTES = clientes_dict
        CACHE_LAST_LOADED = agora
        print(f"Cadastro de {len(clientes_dict)} clientes carregado com sucesso no cache.")
        return CACHE_CLIENTES
    except Exception as e:
        print(f"Erro ao carregar cadastro de clientes do S3: {e}")
        # Se falhar, tenta usar o cache anterior se existir, ou retorna dicionário vazio
        return CACHE_CLIENTES if CACHE_CLIENTES else {}

def calcular_idade(data_nascimento_str):
    """Calcula a idade aproximada com base na data de nascimento (formato YYYY-MM-DD)."""
    try:
        nascimento = datetime.strptime(data_nascimento_str, "%Y-%m-%d")
        hoje = datetime.utcnow()
        return hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
    except Exception:
        return None

def categorizar_faixa_etaria(idade):
    """Categoriza a faixa etária do cliente."""
    if idade is None:
        return "Nao Informado"
    if idade < 18:
        return "Menor de Idade"
    elif idade < 30:
        return "Jovem Adulto (18-29)"
    elif idade < 50:
        return "Adulto (30-49)"
    else:
        return "Senior (50+)"

def lambda_handler(event, context):
    """
    Handler principal do AWS Lambda acionado por eventos do Apache Kafka.
    """
    raw_bucket = os.environ.get("RAW_BUCKET_NAME")
    curated_bucket = os.environ.get("CURATED_BUCKET_NAME")
    
    print(f"Iniciando processamento. Evento recebido: {json.dumps(event)}")
    
    # 1. Carregar cache de clientes para enriquecimento
    clientes = carregar_clientes_do_s3(raw_bucket)
    
    mensagens_processadas = 0
    erros = 0
    
    # 2. Identificar se o evento vem do Kafka (SelfManagedKafka ou MSK)
    # A estrutura contém um dicionário 'records' contendo chaves por 'topico-particao'
    records = event.get("records", {})
    
    if not records:
        print("Nenhum registro Kafka encontrado no evento.")
        return {
            "statusCode": 200,
            "body": json.dumps("Nenhum dado para processar.")
        }
        
    for partition_key, message_list in records.items():
        print(f"Processando particao: {partition_key} contendo {len(message_list)} mensagens.")
        
        for msg in message_list:
            try:
                # Decodificar valor do Kafka (vem codificado em base64)
                value_encoded = msg.get("value", "")
                if not value_encoded:
                    continue
                
                value_decoded = base64.b64decode(value_encoded).decode("utf-8")
                venda = json.loads(value_decoded)
                
                # Exemplo de payload esperado:
                # {
                #   "venda_id": 1001,
                #   "cliente_id": 452,
                #   "produto_id": 102,
                #   "valor": 150.50,
                #   "data_venda": "2026-08-26"
                # }
                
                venda_id = venda.get("venda_id")
                cliente_id = int(venda.get("cliente_id", 0))
                produto_id = venda.get("produto_id")
                valor = float(venda.get("valor", 0.0))
                data_venda = venda.get("data_venda", datetime.utcnow().strftime("%Y-%m-%d"))
                
                # 3. Enriquecer dados fazendo JOIN com clientes
                cliente_info = clientes.get(cliente_id, {})
                nome_cliente = cliente_info.get("nome", "Cliente Desconhecido")
                nascimento = cliente_info.get("data_nascimento", "")
                
                idade = calcular_idade(nascimento) if nascimento else None
                faixa_etaria = categorizar_faixa_etaria(idade)
                
                # Objeto final enriquecido
                transacao_curada = {
                    "venda_id": venda_id,
                    "cliente_id": cliente_id,
                    "nome_cliente": nome_cliente,
                    "idade_cliente": idade,
                    "faixa_etaria": faixa_etaria,
                    "produto_id": produto_id,
                    "valor": valor,
                    "data_venda": data_venda,
                    "processado_em": datetime.utcnow().isoformat()
                }
                
                # 4. Salvar no S3 particionado (Data Lake Hive Partitioning format)
                # Formato: data_venda=YYYY-MM-DD/venda_id.json
                s3_key = f"vendas_detalhadas/data_venda={data_venda}/venda_{venda_id}.json"
                
                print(f"Salvando transacao enriquecida em s3://{curated_bucket}/{s3_key}")
                s3_client.put_object(
                    Bucket=curated_bucket,
                    Key=s3_key,
                    Body=json.dumps(transacao_curada, ensure_ascii=False),
                    ContentType="application/json"
                )
                
                mensagens_processadas += 1
                
            except Exception as e:
                print(f"Erro ao processar mensagem individual: {e}")
                erros += 1
                
    print(f"Processamento concluído. Mensagens processadas: {mensagens_processadas}. Erros: {erros}")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "mensagem": "Sucesso",
            "processadas": mensagens_processadas,
            "erros": erros
        })
    }
