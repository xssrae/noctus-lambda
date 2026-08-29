import unittest
from unittest.mock import patch, MagicMock
import json
import base64
import sys
import os

# Adiciona o diretório src ao path para poder importar a lambda_function
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "src")))

import lambda_function

class TestLambdaETL(unittest.TestCase):

    def setUp(self):
        # Limpar caches de execução anterior
        lambda_function.CACHE_CLIENTES = {}
        lambda_function.CACHE_LAST_LOADED = None
        os.environ["RAW_BUCKET_NAME"] = "test-raw"
        os.environ["CURATED_BUCKET_NAME"] = "test-curated"

    @patch("lambda_function.s3_client")
    def test_carregar_clientes_do_s3(self, mock_s3):
        # Mock do retorno do S3 contendo arquivo clientes.csv
        csv_data = "cliente_id,nome,data_nascimento,email\n1,Carlos Santos,1990-05-15,carlos@email.com\n2,Ana Oliveira,1985-10-20,ana@email.com\n"
        
        mock_response = {
            "Body": MagicMock(read=MagicMock(return_value=csv_data.encode("utf-8")))
        }
        mock_s3.get_object.return_value = mock_response

        # Executar a função
        clientes = lambda_function.carregar_clientes_do_s3("test-raw")

        # Verificar se chamou o S3 corretamente
        mock_s3.get_object.assert_called_once_with(Bucket="test-raw", Key="clientes.csv")
        
        # Verificar o parsing
        self.assertEqual(len(clientes), 2)
        self.assertEqual(clientes[1]["nome"], "Carlos Santos")
        self.assertEqual(clientes[2]["data_nascimento"], "1985-10-20")

    def test_calcular_idade(self):
        # Testa o cálculo da idade (calcula idade com base na data de nascimento)
        # Como a idade muda dependendo do ano corrente (2026 no caso do teste),
        # podemos verificar se o retorno é um número e é coerente.
        idade = lambda_function.calcular_idade("1996-08-23")
        self.assertIsNotNone(idade)
        self.assertGreaterEqual(idade, 29) # Em 2026 fará 30 anos

    def test_categorizar_faixa_etaria(self):
        self.assertEqual(lambda_function.categorizar_faixa_etaria(15), "Menor de Idade")
        self.assertEqual(lambda_function.categorizar_faixa_etaria(25), "Jovem Adulto (18-29)")
        self.assertEqual(lambda_function.categorizar_faixa_etaria(40), "Adulto (30-49)")
        self.assertEqual(lambda_function.categorizar_faixa_etaria(60), "Senior (50+)")

    @patch("lambda_function.s3_client")
    @patch("lambda_function.carregar_clientes_do_s3")
    def test_lambda_handler_sucesso(self, mock_load_clientes, mock_s3):
        # Configurar mocks
        mock_load_clientes.return_value = {
            452: {
                "nome": "Derrek",
                "data_nascimento": "1993-01-01"
            }
        }
        
        # Simular payload do Kafka codificado em base64
        # Mensagem: {"venda_id": 1001, "cliente_id": 452, "produto_id": 102, "valor": 150.50, "data_venda": "2026-08-26"}
        venda_payload = {
            "venda_id": 1001,
            "cliente_id": 452,
            "produto_id": 102,
            "valor": 150.50,
            "data_venda": "2026-08-26"
        }
        venda_str = json.dumps(venda_payload)
        venda_b64 = base64.b64encode(venda_str.encode("utf-8")).decode("utf-8")
        
        # Evento vindo do Kafka para o Lambda
        event = {
            "eventSource": "SelfManagedKafka",
            "records": {
                "vendas-topic-0": [
                    {
                        "topic": "vendas-topic",
                        "partition": 0,
                        "offset": 1234,
                        "value": venda_b64
                    }
                ]
            }
        }
        
        # Chamar handler
        response = lambda_function.lambda_handler(event, None)
        
        # Verificar se retornou status code 200
        self.assertEqual(response["statusCode"], 200)
        
        # Verificar se tentou escrever o arquivo curado no S3
        mock_s3.put_object.assert_called_once()
        
        # Validar os parâmetros de escrita do S3
        chamada_args = mock_s3.put_object.call_args[1]
        self.assertEqual(chamada_args["Bucket"], "test-curated")
        self.assertEqual(chamada_args["Key"], "vendas_detalhadas/data_venda=2026-08-26/venda_1001.json")
        
        # Validar conteúdo salvo
        conteudo_salvo = json.loads(chamada_args["Body"])
        self.assertEqual(conteudo_salvo["venda_id"], 1001)
        self.assertEqual(conteudo_salvo["nome_cliente"], "Derrek")
        self.assertEqual(conteudo_salvo["faixa_etaria"], "Adulto (30-49)") # Em 2026, nascido em 1993 tem 33 anos

if __name__ == "__main__":
    unittest.main()
