# Noctus Lambda

Consumidor Kafka responsavel por validar resultados `FraudAssessment` produzidos pelo `noctus-service` e grava-los na camada S3 Curated.

```text
orders-topic --> noctus-service --> fraud-detection-topic --> Noctus Lambda --> S3 Curated
```

## Contrato de entrada

A Lambda recebe o envelope de Amazon MSK ou Kafka self-managed. Cada `records.<topico-particao>[].value` contem um `FraudAssessment` JSON codificado em Base64.

Campos preservados na Curated:

- `assessmentId`, `sourceEventId`, `orderId`, `customerId`
- `orderAmount`, `currency`, `riskScore`, `decision`, `reasons`
- `assessedAt`, `modelVersion`

O objeto e gravado em:

```text
s3://<bucket>/fraud-assessments/decision=<DECISION>/year=YYYY/month=MM/day=DD/<assessmentId>.json
```

Como a chave usa `assessmentId`, um retry sobrescreve o mesmo objeto em vez de criar duplicatas. Payload invalido ou falha do S3 gera excecao para impedir a confirmacao silenciosa do lote.

## Execucao local integrada

O `docker-compose.yml` fica no repositorio irmao `noctus-service`:

```powershell
Set-Location C:\Projetos\noctus-service
docker compose up --build -d
docker compose logs -f noctus-lambda-worker
```

O worker local consome `fraud-detection-topic` no grupo `noctus-curation-v1`, converte os registros para o mesmo envelope recebido na AWS e somente confirma os offsets depois que todo o lote foi gravado.

Variaveis do worker:

| Variavel | Padrao |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `FRAUD_RESULTS_TOPIC` | `fraud-detection-topic` |
| `KAFKA_CONSUMER_GROUP` | `noctus-curation-v1` |
| `CURATED_BUCKET_NAME` | obrigatoria |
| `CURATED_PREFIX` | `fraud-assessments` |
| `S3_ENDPOINT` | vazio na AWS |

## Payloads de teste

- `examples/fraud-assessment.json`: mensagem direta de `fraud-detection-topic`.
- `examples/lambda-kafka-event.json`: envelope Base64 para teste do handler ou console da Lambda; nao testa a conectividade com o broker.

## Testes Python via Docker

Nao e necessario instalar Python na maquina:

```powershell
docker build -t noctus-lambda-test .
docker run --rm --entrypoint python -e AWS_ACCESS_KEY_ID=test -e AWS_SECRET_ACCESS_KEY=test -e AWS_REGION=us-east-1 -e CURATED_BUCKET_NAME=test-curated noctus-lambda-test /app/test_lambda.py
```

Como alternativa, execute localmente apos instalar `requirements.txt`:

```powershell
pip install -r requirements.txt
python test_lambda.py
```

## Terraform

O trigger Kafka fica desabilitado por padrao, portanto o deploy do restante da infraestrutura nao exige um cluster existente.

### Amazon MSK

```hcl
enable_kafka_trigger  = true
kafka_source_type     = "msk"
kafka_msk_cluster_arn = "arn:aws:kafka:us-east-1:123456789012:cluster/noctus/..."
```

Essa variante assume autenticacao IAM no MSK. As permissoes de leitura ficam limitadas aos recursos de topico e consumer group derivados do ARN informado.

### Kafka self-managed

```hcl
enable_kafka_trigger      = true
kafka_source_type         = "self_managed"
kafka_bootstrap_servers   = ["broker-1.example.com:9092", "broker-2.example.com:9092"]
kafka_subnet_ids          = ["subnet-0123456789abcdef0"]
kafka_security_group_ids  = ["sg-0123456789abcdef0"]
kafka_auth_type           = "SASL_SCRAM_512_AUTH"
kafka_auth_secret_arn     = "arn:aws:secretsmanager:us-east-1:123456789012:secret:noctus-kafka"
```

As duas opcoes usam `fraud-detection-topic`, consumer group `noctus-curation-v1` e uma fila SQS com retencao de 14 dias para lotes descartados. O Terraform apenas configura a conexao; ele nao cria o cluster Kafka/MSK.

Validacao:

```powershell
Set-Location terraform
terraform fmt -check
terraform init
terraform validate
```
