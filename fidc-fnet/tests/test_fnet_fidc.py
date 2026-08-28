"""
Testes offline: nada aqui toca a rede. O cliente recebe uma sessao falsa que
devolve respostas canned no formato que o FNET usa (DataTables + base64).
"""

import base64
import json
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fnet_fidc as fx


# ------------------------------------------------------------------
# Dublês
# ------------------------------------------------------------------

class RespostaFalsa:
    def __init__(self, corpo=b"", payload=None, headers=None, status=200):
        self._payload = payload
        self.content = corpo
        self.text = corpo.decode("utf-8", "replace")
        self.headers = headers or {}
        self.status_code = status

    def json(self):
        if self._payload is None:
            raise ValueError("sem json")
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class SessaoFalsa:
    def __init__(self, respostas):
        self.headers = {}
        self.respostas = respostas
        self.chamadas = []

    def get(self, url, params=None, timeout=None):
        self.chamadas.append((url, dict(params or {})))
        resposta = self.respostas.pop(0)
        return resposta(params) if callable(resposta) else resposta


def doc(**kwargs):
    base = {
        "id": 111,
        "descricaoFundo": "FIDC EXEMPLO",
        "cnpjFundo": "12.345.678/0001-90",
        "categoriaDocumento": "Informe Mensal",
        "tipoDocumento": "Informe Mensal Estruturado",
        "especieDocumento": "",
        "dataReferencia": "07/2026",
        "dataEntrega": "2026-08-12T18:03:00",
        "versao": 1,
        "nomeAdministrador": "ADM EXEMPLO DTVM",
    }
    base.update(kwargs)
    return base


def busca(*docs, total=None):
    linhas = list(docs)
    return RespostaFalsa(payload={"data": linhas, "recordsFiltered": total if total is not None else len(linhas)})


# ------------------------------------------------------------------
# Datas e normalizacao
# ------------------------------------------------------------------

class TestDatas(unittest.TestCase):
    def test_parse_mes_formatos(self):
        self.assertEqual(fx.parse_mes("2026-07"), (2026, 7))
        self.assertEqual(fx.parse_mes("202607"), (2026, 7))
        self.assertEqual(fx.parse_mes("7/2026"), (2026, 7))

    def test_parse_mes_invalido(self):
        for ruim in ("2026-13", "julho", "26-07"):
            with self.assertRaises(Exception):
                fx.parse_mes(ruim)

    def test_mes_anterior_vira_o_ano(self):
        self.assertEqual(fx.mes_anterior(date(2026, 1, 15)), (2025, 12))
        self.assertEqual(fx.mes_anterior(date(2026, 8, 1)), (2026, 7))

    def test_intervalo_de_meses(self):
        self.assertEqual(
            fx.intervalo_de_meses((2025, 11), (2026, 2)),
            [(2025, 11), (2025, 12), (2026, 1), (2026, 2)],
        )
        with self.assertRaises(ValueError):
            fx.intervalo_de_meses((2026, 5), (2026, 4))

    def test_janela_de_busca_cobre_retificacoes(self):
        inicio, fim = fx.janela_de_busca(2026, 11, 3)
        self.assertEqual(inicio, date(2026, 11, 1))
        self.assertEqual(fim, date(2027, 2, 28))

    def test_referencia_do_documento(self):
        self.assertEqual(fx.referencia_do_documento({"dataReferencia": "07/2026"}), (2026, 7))
        self.assertEqual(fx.referencia_do_documento({"dataReferencia": "31/07/2026"}), (2026, 7))
        self.assertEqual(fx.referencia_do_documento({"dataReferencia": "2026-07"}), (2026, 7))
        self.assertIsNone(fx.referencia_do_documento({"dataReferencia": ""}))


class TestFiltro(unittest.TestCase):
    def test_reconhece_informe_com_acento_e_caixa(self):
        d = doc(categoriaDocumento="INFORME MENSAL", tipoDocumento="Anexo 39-I")
        self.assertTrue(fx.e_informe_mensal(d, ["informe mensal"]))

    def test_ignora_outros_documentos(self):
        d = doc(categoriaDocumento="Assembleia", tipoDocumento="Edital de Convocacao")
        self.assertFalse(fx.e_informe_mensal(d, ["informe mensal"]))

    def test_slug_e_cnpj(self):
        self.assertEqual(fx.slugify("FIDC Exemplo Multissetorial - Série 2"), "fidc-exemplo-multissetorial-serie-2")
        self.assertEqual(fx.formatar_cnpj("12345678000190"), "12.345.678/0001-90")
        self.assertEqual(fx.so_digitos("12.345.678/0001-90"), "12345678000190")


# ------------------------------------------------------------------
# Conteudo baixado
# ------------------------------------------------------------------

class TestConteudo(unittest.TestCase):
    def test_decodifica_base64(self):
        xml = b"<?xml version='1.0'?><informe><cnpj>123</cnpj></informe>"
        self.assertEqual(fx.decodificar_conteudo(base64.b64encode(xml)), xml)

    def test_mantem_binario_intacto(self):
        pdf = b"%PDF-1.7\n1 0 obj\n<< >>"
        self.assertEqual(fx.decodificar_conteudo(pdf), pdf)

    def test_mantem_xml_puro(self):
        xml = b"<?xml version='1.0'?><a/>"
        self.assertEqual(fx.decodificar_conteudo(xml), xml)

    def test_extensao_por_content_disposition(self):
        self.assertEqual(fx.adivinhar_extensao(b"qualquer", 'attachment; filename="informe.xml"'), ".xml")

    def test_extensao_por_conteudo(self):
        self.assertEqual(fx.adivinhar_extensao(b"%PDF-1.4 ..."), ".pdf")
        self.assertEqual(fx.adivinhar_extensao(b"<?xml version='1.0'?>"), ".xml")
        self.assertEqual(fx.adivinhar_extensao(b"PK\x03\x04zip"), ".zip")
        self.assertEqual(fx.adivinhar_extensao(b"\x00\x01\x02\x03binario"), ".bin")


# ------------------------------------------------------------------
# Cliente
# ------------------------------------------------------------------

class TestClient(unittest.TestCase):
    def _client(self, respostas):
        return fx.FnetClient(session=SessaoFalsa(respostas), intervalo=0, backoff=0)

    def test_busca_pagina_ate_o_fim(self):
        pagina1 = busca(*[doc(id=i) for i in range(100)], total=150)
        pagina2 = busca(*[doc(id=100 + i) for i in range(50)], total=150)
        client = self._client([pagina1, pagina2])
        docs = list(client.buscar(cnpj="12345678000190", data_inicial=date(2026, 7, 1)))
        self.assertEqual(len(docs), 150)
        self.assertEqual(client.session.chamadas[1][1]["s"], 100)
        self.assertEqual(client.session.chamadas[0][1]["dataInicial"], "01/07/2026")

    def test_busca_manda_tipo_fundo_e_cnpj(self):
        client = self._client([busca(doc())])
        list(client.buscar(tipo_fundo=2, cnpj="12345678000190"))
        _, params = client.session.chamadas[0]
        self.assertEqual(params["tipoFundo"], 2)
        self.assertEqual(params["cnpjFundo"], "12345678000190")
        self.assertEqual(params["o[0][dataEntrega]"], "desc")

    def test_retry_em_erro_5xx(self):
        client = self._client([RespostaFalsa(status=503), busca(doc())])
        docs = list(client.buscar())
        self.assertEqual(len(docs), 1)
        self.assertEqual(len(client.session.chamadas), 2)

    def test_desiste_depois_das_tentativas(self):
        client = self._client([RespostaFalsa(status=500) for _ in range(fx.TENTATIVAS)])
        with self.assertRaises(fx.FnetError):
            list(client.buscar())

    def test_baixar_decodifica_e_nomeia(self):
        xml = b"<?xml version='1.0'?><informe/>"
        resposta = RespostaFalsa(
            corpo=base64.b64encode(xml),
            headers={"Content-Disposition": 'attachment; filename="informe_mensal.xml"'},
        )
        client = self._client([resposta])
        conteudo, extensao = client.baixar(999)
        self.assertEqual(conteudo, xml)
        self.assertEqual(extensao, ".xml")


# ------------------------------------------------------------------
# Fluxo completo
# ------------------------------------------------------------------

class TestProcessar(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.TemporaryDirectory()
        self.destino = Path(self.tmp.name)
        self.addCleanup(self.tmp.cleanup)
        self.cfg = fx.Config(tipo_fundo=2, padroes=["informe mensal"], janela_meses=3)
        self.fundo = fx.Fundo(nome="FIDC Exemplo", cnpj="12.345.678/0001-90", apelido="exemplo")

    def _rodar(self, respostas, **kwargs):
        client = fx.FnetClient(session=SessaoFalsa(respostas), intervalo=0, backoff=0)
        manifest = fx.Manifest(self.destino / fx.MANIFEST_NOME)
        contadores = fx.processar(
            client, self.cfg, [self.fundo], [(2026, 7)], self.destino, manifest, **kwargs
        )
        manifest.salvar()
        return contadores, manifest

    def test_baixa_apenas_o_informe_do_mes(self):
        respostas = [
            busca(
                doc(id=1),
                doc(id=2, categoriaDocumento="Assembleia", tipoDocumento="Ata"),
                doc(id=3, dataReferencia="06/2026"),
            ),
            RespostaFalsa(corpo=base64.b64encode(b"<?xml version='1.0'?><informe/>")),
        ]
        contadores, manifest = self._rodar(respostas)
        self.assertEqual(contadores["baixados"], 1)
        self.assertEqual(contadores["encontrados"], 1)
        salvos = sorted(p.name for p in (self.destino / "2026" / "07" / "exemplo").glob("*"))
        self.assertEqual(salvos, ["2026-07_exemplo_1.xml"])
        registro = manifest.documentos["1"]
        self.assertEqual(registro["referencia"], "2026-07")
        self.assertEqual(registro["cnpj"], "12.345.678/0001-90")
        self.assertEqual(registro["arquivo"], "2026/07/exemplo/2026-07_exemplo_1.xml")

    def test_nao_rebaixa_o_que_ja_esta_no_manifesto(self):
        self._rodar([busca(doc(id=1)), RespostaFalsa(corpo=b"<?xml version='1.0'?><a/>")])
        contadores, _ = self._rodar([busca(doc(id=1))])
        self.assertEqual(contadores["baixados"], 0)
        self.assertEqual(contadores["pulados"], 1)

    def test_retificacao_com_versao_maior_e_rebaixada(self):
        self._rodar([busca(doc(id=1)), RespostaFalsa(corpo=b"<?xml version='1.0'?><a/>")])
        contadores, manifest = self._rodar(
            [busca(doc(id=1, versao=2)), RespostaFalsa(corpo=b"<?xml version='1.0'?><b/>")]
        )
        self.assertEqual(contadores["baixados"], 1)
        self.assertEqual(manifest.documentos["1"]["versao"], 2)
        self.assertTrue((self.destino / "2026" / "07" / "exemplo" / "2026-07_exemplo_1_v2.xml").exists())

    def test_dry_run_nao_escreve_arquivo(self):
        contadores, _ = self._rodar([busca(doc(id=1))], dry_run=True)
        self.assertEqual(contadores["baixados"], 0)
        self.assertEqual(contadores["encontrados"], 1)
        self.assertFalse((self.destino / "2026").exists())

    def test_erro_de_download_nao_derruba_a_rodada(self):
        respostas = [busca(doc(id=1))] + [RespostaFalsa(status=500) for _ in range(fx.TENTATIVAS)]
        contadores, _ = self._rodar(respostas)
        self.assertEqual(contadores["erros"], 1)
        self.assertEqual(contadores["baixados"], 0)

    def test_tenta_cnpj_mascarado_quando_digitos_nao_retornam_nada(self):
        respostas = [busca(), busca(doc(id=7)), RespostaFalsa(corpo=b"<?xml version='1.0'?><a/>")]
        client = fx.FnetClient(session=SessaoFalsa(respostas), intervalo=0, backoff=0)
        manifest = fx.Manifest(self.destino / fx.MANIFEST_NOME)
        contadores = fx.processar(client, self.cfg, [self.fundo], [(2026, 7)], self.destino, manifest)
        self.assertEqual(contadores["baixados"], 1)
        self.assertEqual(client.session.chamadas[0][1]["cnpjFundo"], "12345678000190")
        self.assertEqual(client.session.chamadas[1][1]["cnpjFundo"], "12.345.678/0001-90")


class TestConfig(unittest.TestCase):
    def test_le_fundos_e_ignora_entrada_sem_cnpj(self):
        import tempfile, textwrap
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as fh:
            fh.write(textwrap.dedent("""
                tipo_fundo: 2
                janela_meses: 2
                fundos:
                  - nome: "FIDC A"
                    cnpj: "12.345.678/0001-90"
                    apelido: "a"
                  - nome: "Sem CNPJ"
                  - "98765432000110"
            """))
            caminho = Path(fh.name)
        cfg = fx.carregar_config(caminho)
        caminho.unlink()
        self.assertEqual(cfg.janela_meses, 2)
        self.assertEqual([f.cnpj_digitos for f in cfg.fundos], ["12345678000190", "98765432000110"])
        self.assertEqual(cfg.fundos[0].pasta, "a")


if __name__ == "__main__":
    unittest.main(verbosity=2)
