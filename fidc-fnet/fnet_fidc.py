#!/usr/bin/env python3
"""
fnet-fidc - baixa os Informes Mensais de FIDCs publicados no Fundos.NET (B3/CVM).

O Fundos.NET nao tem API publica documentada. O que existe e o endpoint que a
propria tela de consulta (https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM)
chama por tras: um JSON no formato DataTables. Este script usa dois endpoints:

  GET /fnet/publico/pesquisarGerenciadorDocumentosDados  -> lista de documentos (JSON)
  GET /fnet/publico/downloadDocumento?id=<id>            -> o arquivo em si

Como os ids de categoria/tipo de documento mudam de tempos em tempos, a busca
aqui e feita por CNPJ + janela de data de entrega, e o filtro de "e informe
mensal?" e aplicado no cliente, em cima do texto que o proprio FNET devolve
(campos categoriaDocumento / tipoDocumento). E mais lento que filtrar no
servidor, mas nao quebra quando a B3 renumera os ids.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import re
import sys
import time
import unicodedata
from calendar import monthrange
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

try:
    import requests
except ImportError:  # pragma: no cover - ambiente sem dependencia
    sys.exit("Faltando dependencia: pip install requests")

try:
    import yaml
except ImportError:  # pragma: no cover - ambiente sem dependencia
    sys.exit("Faltando dependencia: pip install PyYAML")


# ============================================================
# Configuracao
# ============================================================

BASE = "https://fnet.bmfbovespa.com.br/fnet/publico"
SEARCH_URL = f"{BASE}/pesquisarGerenciadorDocumentosDados"
DOWNLOAD_URL = f"{BASE}/downloadDocumento"
VIEW_URL = f"{BASE}/exibirDocumento"

# No seletor "Tipo de fundo" do FNET: 1 = FII, 2 = FIDC, 4 = FIP.
# Fica configuravel (fundos.yaml / --tipo-fundo) porque a B3 ja mexeu nisso.
TIPO_FUNDO_FIDC = 2

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

PAGE_SIZE = 100
MAX_PAGINAS = 200          # trava de seguranca contra paginacao infinita
TIMEOUT = 60
TENTATIVAS = 4             # 1 tentativa + 3 retries com backoff 2s/4s/8s
INTERVALO_PADRAO = 0.8     # segundos entre requisicoes (educacao com o servidor)

# Padroes que identificam o Informe Mensal de FIDC. Casam contra
# categoriaDocumento + tipoDocumento + especieDocumento concatenados e
# normalizados (sem acento, minusculo).
PADROES_INFORME_MENSAL = [r"informe\s+mensal"]

CONFIG_PADRAO = Path(__file__).parent / "fundos.yaml"
DEST_PADRAO = Path(__file__).parent / "data"
MANIFEST_NOME = "manifest.json"

log = logging.getLogger("fnet-fidc")


# ============================================================
# Utilidades
# ============================================================

def normalizar(texto: str) -> str:
    """Minusculo, sem acento - para comparar rotulos vindos do FNET."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", sem_acento).strip().lower()


def slugify(texto: str, limite: int = 60) -> str:
    base = normalizar(texto)
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    return (base[:limite].rstrip("-")) or "sem-nome"


def so_digitos(valor: str) -> str:
    return re.sub(r"\D", "", valor or "")


def formatar_cnpj(cnpj: str) -> str:
    d = so_digitos(cnpj).zfill(14)
    return f"{d[:2]}.{d[2:5]}.{d[5:8]}/{d[8:12]}-{d[12:14]}"


def parse_mes(texto: str) -> tuple[int, int]:
    """Aceita 2026-07, 07/2026 ou 202607."""
    t = (texto or "").strip()
    m = re.fullmatch(r"(\d{4})-(\d{1,2})", t) or re.fullmatch(r"(\d{4})(\d{2})", t)
    if m:
        ano, mes = int(m.group(1)), int(m.group(2))
    else:
        m = re.fullmatch(r"(\d{1,2})/(\d{4})", t)
        if not m:
            raise argparse.ArgumentTypeError(f"Mes invalido: {texto!r} (use AAAA-MM)")
        ano, mes = int(m.group(2)), int(m.group(1))
    if not 1 <= mes <= 12:
        raise argparse.ArgumentTypeError(f"Mes invalido: {texto!r}")
    return ano, mes


def mes_anterior(hoje: Optional[date] = None) -> tuple[int, int]:
    hoje = hoje or date.today()
    return (hoje.year - 1, 12) if hoje.month == 1 else (hoje.year, hoje.month - 1)


def somar_meses(ano: int, mes: int, n: int) -> tuple[int, int]:
    total = (ano * 12 + (mes - 1)) + n
    return total // 12, total % 12 + 1


def intervalo_de_meses(inicio: tuple[int, int], fim: tuple[int, int]) -> list[tuple[int, int]]:
    if (fim[0], fim[1]) < (inicio[0], inicio[1]):
        raise ValueError("Mes final anterior ao inicial")
    meses, atual = [], inicio
    while atual <= fim:
        meses.append(atual)
        atual = somar_meses(*atual, 1)
    return meses


def ultimo_dia(ano: int, mes: int) -> int:
    return monthrange(ano, mes)[1]


def referencia_do_documento(doc: dict) -> Optional[tuple[int, int]]:
    """Extrai (ano, mes) de dataReferencia, que vem como MM/AAAA ou DD/MM/AAAA."""
    bruto = (doc.get("dataReferencia") or "").strip()
    if not bruto:
        return None
    m = re.search(r"(?:(\d{1,2})/)?(\d{1,2})/(\d{4})", bruto)
    if m:
        return int(m.group(3)), int(m.group(2))
    m = re.fullmatch(r"(\d{4})-(\d{1,2})(?:-\d{1,2})?", bruto)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None


def rotulo_documento(doc: dict) -> str:
    partes = [doc.get("categoriaDocumento"), doc.get("tipoDocumento"), doc.get("especieDocumento")]
    return normalizar(" ".join(p for p in partes if p))


def e_informe_mensal(doc: dict, padroes: Iterable[str]) -> bool:
    rotulo = rotulo_documento(doc)
    return any(re.search(p, rotulo) for p in padroes)


# ============================================================
# Config (fundos.yaml)
# ============================================================

@dataclass
class Fundo:
    nome: str
    cnpj: str
    apelido: str = ""

    @property
    def cnpj_digitos(self) -> str:
        return so_digitos(self.cnpj)

    @property
    def pasta(self) -> str:
        return slugify(self.apelido or self.nome)


@dataclass
class Config:
    tipo_fundo: int = TIPO_FUNDO_FIDC
    padroes: list[str] = field(default_factory=lambda: list(PADROES_INFORME_MENSAL))
    janela_meses: int = 3
    fundos: list[Fundo] = field(default_factory=list)


def carregar_config(caminho: Path) -> Config:
    if not caminho.exists():
        raise SystemExit(f"Config nao encontrada: {caminho}")
    dados = yaml.safe_load(caminho.read_text(encoding="utf-8")) or {}
    fundos = []
    for item in dados.get("fundos") or []:
        if isinstance(item, str):                     # so o CNPJ, sem nome
            fundos.append(Fundo(nome=item, cnpj=item))
            continue
        cnpj = str(item.get("cnpj") or "").strip()
        if not so_digitos(cnpj):
            log.warning("Fundo sem CNPJ valido ignorado: %r", item)
            continue
        fundos.append(
            Fundo(
                nome=str(item.get("nome") or cnpj).strip(),
                cnpj=cnpj,
                apelido=str(item.get("apelido") or "").strip(),
            )
        )
    return Config(
        tipo_fundo=int(dados.get("tipo_fundo") or TIPO_FUNDO_FIDC),
        padroes=[normalizar(p) for p in (dados.get("padroes") or PADROES_INFORME_MENSAL)],
        janela_meses=int(dados.get("janela_meses") or 3),
        fundos=fundos,
    )


# ============================================================
# Cliente do FNET
# ============================================================

class FnetError(RuntimeError):
    pass


class FnetClient:
    def __init__(
        self,
        session=None,
        intervalo: float = INTERVALO_PADRAO,
        timeout: int = TIMEOUT,
        backoff: float = 2.0,
    ):
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{BASE}/abrirGerenciadorDocumentosCVM",
            }
        )
        self.intervalo = intervalo
        self.timeout = timeout
        self.backoff = backoff  # base do backoff exponencial; 0 desliga (testes)
        self._ultima_requisicao = 0.0

    # -- infra -------------------------------------------------

    def _esperar(self) -> None:
        gasto = time.monotonic() - self._ultima_requisicao
        if self._ultima_requisicao and gasto < self.intervalo:
            time.sleep(self.intervalo - gasto)
        self._ultima_requisicao = time.monotonic()

    def _get(self, url: str, params: dict) -> Any:
        erro: Optional[Exception] = None
        for tentativa in range(TENTATIVAS):
            if tentativa:
                espera = self.backoff * (2 ** (tentativa - 1))
                if espera:
                    log.warning("Tentativa %d falhou (%s); aguardando %gs", tentativa, erro, espera)
                    time.sleep(espera)
            self._esperar()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
                if resp.status_code >= 500 or resp.status_code == 429:
                    raise FnetError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                return resp
            except Exception as exc:  # noqa: BLE001 - rede: qualquer falha vira retry
                erro = exc
        raise FnetError(f"Falha ao chamar {url}: {erro}")

    # -- busca -------------------------------------------------

    def buscar(
        self,
        *,
        tipo_fundo: int = TIPO_FUNDO_FIDC,
        cnpj: str = "",
        data_inicial: Optional[date] = None,
        data_final: Optional[date] = None,
        page_size: int = PAGE_SIZE,
    ) -> Iterator[dict]:
        """Itera os documentos que o FNET devolve para o filtro dado."""
        inicio = 0
        for pagina in range(MAX_PAGINAS):
            params = {
                "d": 1,
                "s": inicio,
                "l": page_size,
                "o[0][dataEntrega]": "desc",
                "tipoFundo": tipo_fundo,
                "idCategoriaDocumento": 0,
                "idTipoDocumento": 0,
                "idEspecieDocumento": 0,
                "situacao": "A",
            }
            if cnpj:
                params["cnpjFundo"] = cnpj
            if data_inicial:
                params["dataInicial"] = data_inicial.strftime("%d/%m/%Y")
            if data_final:
                params["dataFinal"] = data_final.strftime("%d/%m/%Y")

            resp = self._get(SEARCH_URL, params)
            try:
                payload = resp.json()
            except ValueError as exc:
                raise FnetError(f"Resposta nao-JSON da busca: {resp.text[:200]!r}") from exc

            linhas = payload.get("data") or []
            for doc in linhas:
                yield doc

            total = payload.get("recordsFiltered") or payload.get("recordsTotal") or 0
            inicio += len(linhas)
            if len(linhas) < page_size or inicio >= int(total):
                return
            log.debug("Pagina %d: %d docs (total %s)", pagina + 1, len(linhas), total)

    # -- download ----------------------------------------------

    def baixar(self, id_documento: int | str) -> tuple[bytes, str]:
        """Devolve (conteudo, extensao) do documento."""
        resp = self._get(DOWNLOAD_URL, {"id": id_documento})
        conteudo = decodificar_conteudo(resp.content)
        extensao = adivinhar_extensao(conteudo, resp.headers.get("Content-Disposition", ""))
        return conteudo, extensao


def decodificar_conteudo(bruto: bytes) -> bytes:
    """
    O downloadDocumento do FNET costuma devolver o arquivo em base64 (texto puro,
    sem cabecalho). Quando o corpo so tem caracteres de base64, decodifica; se
    ja vier binario (PDF, ZIP, XML), devolve como esta.
    """
    limpo = (bruto or b"").strip()
    if not limpo or len(limpo) < 8:
        return bruto
    if not re.fullmatch(rb"[A-Za-z0-9+/=\r\n]+", limpo):
        return bruto
    try:
        decodificado = base64.b64decode(limpo, validate=True)
    except Exception:  # noqa: BLE001 - base64 malformado: fica com o original
        return bruto
    return decodificado or bruto


def adivinhar_extensao(conteudo: bytes, content_disposition: str = "") -> str:
    m = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", content_disposition or "")
    if m:
        sufixo = Path(m.group(1).strip()).suffix.lower()
        if 1 < len(sufixo) <= 6:
            return sufixo
    inicio = (conteudo or b"")[:512].lstrip()
    if inicio.startswith(b"%PDF"):
        return ".pdf"
    if inicio.startswith(b"PK\x03\x04"):
        return ".zip"
    if inicio.startswith(b"<?xml") or inicio.startswith(b"<"):
        return ".xml"
    if inicio.startswith(b"{") or inicio.startswith(b"["):
        return ".json"
    return ".bin"


# ============================================================
# Manifesto (o que ja foi baixado)
# ============================================================

class Manifest:
    def __init__(self, caminho: Path):
        self.caminho = caminho
        self.documentos: dict[str, dict] = {}
        if caminho.exists():
            try:
                dados = json.loads(caminho.read_text(encoding="utf-8"))
                self.documentos = dados.get("documentos") or {}
            except (ValueError, OSError) as exc:
                log.warning("Manifesto ilegivel (%s); comecando do zero", exc)

    def ja_tem(self, doc: dict) -> bool:
        registro = self.documentos.get(str(doc.get("id")))
        if not registro:
            return False
        # Retificacao chega com o mesmo id e versao maior - vale rebaixar.
        versao_nova = int(doc.get("versao") or 1)
        if versao_nova > int(registro.get("versao") or 1):
            return False
        return Path(registro.get("arquivo", "")).exists() or bool(registro.get("arquivo"))

    def registrar(self, doc: dict, fundo: Fundo, arquivo: Path, conteudo: bytes, raiz: Path) -> None:
        ano_mes = referencia_do_documento(doc)
        self.documentos[str(doc.get("id"))] = {
            "id": doc.get("id"),
            "fundo": fundo.nome,
            "cnpj": formatar_cnpj(fundo.cnpj) if fundo.cnpj_digitos else fundo.cnpj,
            "referencia": f"{ano_mes[0]:04d}-{ano_mes[1]:02d}" if ano_mes else None,
            "categoria": doc.get("categoriaDocumento"),
            "tipo": doc.get("tipoDocumento"),
            "versao": int(doc.get("versao") or 1),
            "data_entrega": doc.get("dataEntrega"),
            "administrador": doc.get("nomeAdministrador"),
            "arquivo": str(arquivo.relative_to(raiz)) if arquivo.is_relative_to(raiz) else str(arquivo),
            "bytes": len(conteudo),
            "sha256": hashlib.sha256(conteudo).hexdigest(),
            "url": f"{VIEW_URL}?id={doc.get('id')}&cvm=true",
            "baixado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
        }

    def salvar(self) -> None:
        self.caminho.parent.mkdir(parents=True, exist_ok=True)
        ordenado = dict(sorted(self.documentos.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 0))
        payload = {
            "gerado_em": datetime.now().astimezone().isoformat(timespec="seconds"),
            "total": len(ordenado),
            "documentos": ordenado,
        }
        self.caminho.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


# ============================================================
# Orquestracao
# ============================================================

def janela_de_busca(ano: int, mes: int, janela_meses: int) -> tuple[date, date]:
    """
    O informe do mes M e entregue ate ~15 dias depois do fechamento, mas
    retificacoes aparecem meses depois. Busca de 01/M ate o fim de M+janela.
    """
    inicio = date(ano, mes, 1)
    ano_fim, mes_fim = somar_meses(ano, mes, max(janela_meses, 0))
    return inicio, date(ano_fim, mes_fim, ultimo_dia(ano_fim, mes_fim))


def caminho_do_arquivo(raiz: Path, fundo: Fundo, doc: dict, ano: int, mes: int, extensao: str) -> Path:
    versao = int(doc.get("versao") or 1)
    sufixo_versao = f"_v{versao}" if versao > 1 else ""
    nome = f"{ano:04d}-{mes:02d}_{fundo.pasta}_{doc.get('id')}{sufixo_versao}{extensao}"
    return raiz / f"{ano:04d}" / f"{mes:02d}" / fundo.pasta / nome


def coletar_mes(
    client: FnetClient,
    cfg: Config,
    fundo: Fundo,
    ano: int,
    mes: int,
) -> list[dict]:
    """Documentos de informe mensal do fundo com dataReferencia == ano/mes."""
    inicio, fim = janela_de_busca(ano, mes, cfg.janela_meses)
    encontrados: dict[str, dict] = {}

    # Alguns ambientes do FNET aceitam CNPJ so com digitos, outros formatado.
    # Tenta digitos e, se nao vier nada, tenta a mascara antes de desistir.
    tentativas = [fundo.cnpj_digitos] if fundo.cnpj_digitos else [fundo.cnpj]
    if fundo.cnpj_digitos:
        tentativas.append(formatar_cnpj(fundo.cnpj))

    for cnpj in tentativas:
        vistos = 0
        for doc in client.buscar(
            tipo_fundo=cfg.tipo_fundo, cnpj=cnpj, data_inicial=inicio, data_final=fim
        ):
            vistos += 1
            if not e_informe_mensal(doc, cfg.padroes):
                continue
            if referencia_do_documento(doc) != (ano, mes):
                continue
            encontrados[str(doc.get("id"))] = doc
        log.debug("%s | CNPJ %s: %d documentos na janela", fundo.nome, cnpj, vistos)
        if vistos:
            break

    return sorted(encontrados.values(), key=lambda d: int(d.get("versao") or 1))


def processar(
    client: FnetClient,
    cfg: Config,
    fundos: list[Fundo],
    meses: list[tuple[int, int]],
    destino: Path,
    manifest: Manifest,
    *,
    dry_run: bool = False,
    forcar: bool = False,
) -> dict[str, int]:
    contadores = {"encontrados": 0, "baixados": 0, "pulados": 0, "erros": 0}

    for ano, mes in meses:
        for fundo in fundos:
            try:
                docs = coletar_mes(client, cfg, fundo, ano, mes)
            except FnetError as exc:
                log.error("%s | %04d-%02d: busca falhou: %s", fundo.nome, ano, mes, exc)
                contadores["erros"] += 1
                continue

            if not docs:
                log.info("%s | %04d-%02d: nenhum informe mensal encontrado", fundo.nome, ano, mes)
                continue

            contadores["encontrados"] += len(docs)
            for doc in docs:
                if not forcar and manifest.ja_tem(doc):
                    contadores["pulados"] += 1
                    log.debug("%s | doc %s ja baixado", fundo.nome, doc.get("id"))
                    continue
                if dry_run:
                    log.info(
                        "[dry-run] %s | %04d-%02d | doc %s (%s, v%s)",
                        fundo.nome, ano, mes, doc.get("id"),
                        doc.get("categoriaDocumento"), doc.get("versao"),
                    )
                    continue
                try:
                    conteudo, extensao = client.baixar(doc["id"])
                except (FnetError, KeyError) as exc:
                    log.error("%s | doc %s: download falhou: %s", fundo.nome, doc.get("id"), exc)
                    contadores["erros"] += 1
                    continue

                arquivo = caminho_do_arquivo(destino, fundo, doc, ano, mes, extensao)
                arquivo.parent.mkdir(parents=True, exist_ok=True)
                arquivo.write_bytes(conteudo)
                manifest.registrar(doc, fundo, arquivo, conteudo, destino)
                contadores["baixados"] += 1
                log.info(
                    "%s | %04d-%02d | %s (%d KB)",
                    fundo.nome, ano, mes, arquivo.name, len(conteudo) // 1024,
                )

    return contadores


def descobrir_fundos(client: FnetClient, cfg: Config, ano: int, mes: int) -> list[Fundo]:
    """Modo --todos: varre a janela inteira e monta a lista de fundos do zero."""
    inicio, fim = janela_de_busca(ano, mes, cfg.janela_meses)
    fundos: dict[str, Fundo] = {}
    for doc in client.buscar(tipo_fundo=cfg.tipo_fundo, data_inicial=inicio, data_final=fim):
        if not e_informe_mensal(doc, cfg.padroes):
            continue
        if referencia_do_documento(doc) != (ano, mes):
            continue
        cnpj = so_digitos(doc.get("cnpjFundo") or "")
        nome = doc.get("descricaoFundo") or doc.get("nomePregao") or cnpj
        if cnpj and cnpj not in fundos:
            fundos[cnpj] = Fundo(nome=nome, cnpj=cnpj)
    log.info("Descobertos %d FIDCs com informe em %04d-%02d", len(fundos), ano, mes)
    return list(fundos.values())


# ============================================================
# CLI
# ============================================================

def montar_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Baixa Informes Mensais de FIDCs do Fundos.NET (B3/CVM).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Exemplos:\n"
            "  python fnet_fidc.py                        # mes anterior, fundos do fundos.yaml\n"
            "  python fnet_fidc.py --mes 2026-07\n"
            "  python fnet_fidc.py --desde 2026-01 --ate 2026-06\n"
            "  python fnet_fidc.py --cnpj 12.345.678/0001-90 --mes 2026-07\n"
            "  python fnet_fidc.py --mes 2026-07 --todos --dry-run\n"
        ),
    )
    p.add_argument("--mes", type=parse_mes, help="Mes de referencia (AAAA-MM). Padrao: mes anterior.")
    p.add_argument("--desde", type=parse_mes, help="Inicio do intervalo (AAAA-MM).")
    p.add_argument("--ate", type=parse_mes, help="Fim do intervalo (AAAA-MM). Padrao: --desde.")
    p.add_argument("--config", type=Path, default=CONFIG_PADRAO, help="Arquivo de fundos (padrao: fundos.yaml).")
    p.add_argument("--cnpj", action="append", default=[], help="CNPJ avulso (pode repetir); ignora o fundos.yaml.")
    p.add_argument("--todos", action="store_true", help="Todos os FIDCs que publicaram no mes (volume alto).")
    p.add_argument("--out", type=Path, default=DEST_PADRAO, help="Pasta de destino (padrao: data/).")
    p.add_argument("--tipo-fundo", type=int, help="Sobrescreve tipoFundo do FNET (2 = FIDC).")
    p.add_argument("--janela-meses", type=int, help="Meses apos a referencia para procurar entregas/retificacoes.")
    p.add_argument("--intervalo", type=float, default=INTERVALO_PADRAO, help="Pausa entre requisicoes, em segundos.")
    p.add_argument("--force", action="store_true", help="Rebaixa mesmo o que ja esta no manifesto.")
    p.add_argument("--dry-run", action="store_true", help="Lista o que baixaria, sem baixar.")
    p.add_argument("-v", "--verbose", action="store_true", help="Log detalhado.")
    return p


def resolver_meses(args) -> list[tuple[int, int]]:
    if args.desde:
        return intervalo_de_meses(args.desde, args.ate or args.desde)
    if args.mes:
        return [args.mes]
    return [mes_anterior()]


def main(argv: Optional[list[str]] = None) -> int:
    args = montar_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )

    cfg = carregar_config(args.config) if args.config.exists() else Config()
    if args.tipo_fundo:
        cfg.tipo_fundo = args.tipo_fundo
    if args.janela_meses is not None:
        cfg.janela_meses = args.janela_meses

    try:
        meses = resolver_meses(args)
    except ValueError as exc:
        log.error("%s", exc)
        return 2

    destino: Path = args.out
    destino.mkdir(parents=True, exist_ok=True)
    manifest = Manifest(destino / MANIFEST_NOME)
    client = FnetClient(intervalo=args.intervalo)

    if args.cnpj:
        fundos = [Fundo(nome=formatar_cnpj(c), cnpj=c) for c in args.cnpj]
    elif args.todos:
        fundos = descobrir_fundos(client, cfg, *meses[0])
    else:
        fundos = cfg.fundos

    if not fundos:
        log.error(
            "Nenhum fundo para consultar. Preencha a lista em %s, use --cnpj ou rode com --todos.",
            args.config,
        )
        return 2

    log.info(
        "Mes(es): %s | fundos: %d | destino: %s",
        ", ".join(f"{a:04d}-{m:02d}" for a, m in meses), len(fundos), destino,
    )

    contadores = processar(
        client, cfg, fundos, meses, destino, manifest,
        dry_run=args.dry_run, forcar=args.force,
    )

    if not args.dry_run:
        manifest.salvar()

    log.info(
        "Fim: %d encontrados, %d baixados, %d ja tinha, %d erros",
        contadores["encontrados"], contadores["baixados"],
        contadores["pulados"], contadores["erros"],
    )
    return 1 if contadores["erros"] and not contadores["baixados"] else 0


if __name__ == "__main__":
    sys.exit(main())
