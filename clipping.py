#!/usr/bin/env python3
"""
Clipping Diario - Itau BBA Consumer
Puxa noticias de varios veiculos, filtra pela cobertura (lida de keywords.yaml)
e gera um digest ordenado por relevancia.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

try:
    import feedparser
except ImportError:
    sys.exit("Faltando dependencia: pip install feedparser")

try:
    import yaml
except ImportError:
    sys.exit("Faltando dependencia: pip install pyyaml")

try:
    import requests
    from bs4 import BeautifulSoup
    HAS_HTML = True
except ImportError:
    HAS_HTML = False


# ============================================================
# Configuracao
# ============================================================

KEYWORDS_FILE = Path(__file__).parent / "keywords.yaml"
SEEN_DB = Path.home() / ".valor_clipping_seen.json"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"

FEED_TIMEOUT = 10

# Scoring por região do texto (reduz falso positivo de RSS longo)
LEAD_CHARS = 500
BODY_CHARS = 3000
ECONOMY_SECTOR = "Economia"

JUNK_TITLES = {
    "curtas", "giro", "resumo", "resumo do dia", "giro do dia",
    "painel", "noticias em tempo real", "últimas notícias",
    "a hora", "a hora do mercado", "panorama",
}

GLOBAL_SECTOR_NAME = "Global Consumer"

BR_ONLY_SECTORS = {
    "Fashion", "Joias", "E-commerce", "Varejo Alimentar", "Academias",
    "Cosméticos", "Farmácias", "Economia", "Material de Construção",
    "Eletrônicos", "Viagens", "Pet", "Wellness e Esportes",
}

# Limita excesso de matérias de tópicos recorrentes no JSON final.
TOPIC_CAPS = {
    "jornada_trabalho": 12,
    "desenrola": 8,
    "reforma_tributaria": 10,
}

# Se a matéria cita diretamente empresas cobertas no título, ela não deve
# ser limitada pelo cap de tópico.
TOPIC_CAP_BYPASS_TERMS = {
    "mercado livre", "mercadolibre", "meli", "mercado pago",
    "magalu", "mglu", "casas bahia", "amazon", "shopee",
    "shein", "temu", "aliexpress",
    "renner", "lren", "c&a", "ceab", "riachuelo", "guararapes",
    "azzas", "arezzo", "hering", "havan", "farm rio",
    "assai", "assaí", "gpa", "pao de acucar", "pão de açúcar",
    "carrefour", "atacadao", "atacadão", "grupo mateus",
    "rd saude", "rd saúde", "raia", "drogasil", "pague menos",
    "panvel", "natura", "boticario", "boticário", "vivara",
    "track&field", "track field", "smart fit", "petz", "cobasi",
}
# Nota: termos genéricos demais (reserva, farm, life, soma) foram retirados
# do bypass de propósito. Como substring/palavra eles casavam contexto errado
# ("reserva de energia", "farmácia", "lifestyle", "somando") e furavam o cap
# por engano. As marcas reais (Farm, Reserva) entram no clipping pelo próprio
# matching de setor (Fashion), que não tem topic capado, então o bypass é
# irrelevante para elas. "farm rio" fica como termo inequívoco da marca.

# Regex de bypass com fronteira de palavra. Usamos (?<!\w) ... (?!\w) em vez
# de \b porque alguns termos têm "&" (c&a, track&field), onde \b se comporta
# de forma inesperada.
_TOPIC_CAP_BYPASS_RE = re.compile(
    "|".join(
        rf"(?<!\w){re.escape(_t)}(?!\w)"
        for _t in sorted(TOPIC_CAP_BYPASS_TERMS, key=len, reverse=True)
    )
)

# Dominios bloqueados - URLs desses sites sao descartadas
# UOL puro foi removido pois puxa muitas materias antigas.
# Folha (folha.uol.com.br) NAO eh bloqueada - eh outro veiculo.
_BLOCKED_URL_RE = re.compile(
    r"(noticias\.uol\.com\.br|economia\.uol\.com\.br|esporte\.uol\.com\.br|"
    r"entretenimento\.uol\.com\.br|www\.uol\.com\.br|rss\.uol\.com\.br|"
    r"^https?://uol\.com\.br|//uol\.com\.br)",
    re.IGNORECASE
)


def is_blocked_url(url: str) -> bool:
    """Retorna True se a URL eh de um dominio bloqueado."""
    if not url:
        return False
    if 'folha.uol.com.br' in url:
        return False
    return bool(_BLOCKED_URL_RE.search(url))


# ============================================================
# Carrega keywords
# ============================================================

def _compile_context_term(term_n: str):
    """
    Compila um termo de contexto (requires_any/requires_none) com tolerância
    a plural. Cada palavra alfabética com 4+ letras ganha um 's?' opcional no
    fim, então 'loja' casa 'loja' e 'lojas', 'hipermercado' casa o plural etc.

    Só mexe em termos de CONTEXTO, nunca no alias principal (que é exato).
    Palavras curtas (<4 letras), com número, ou já terminadas em 's' ficam
    como estão, pra evitar falsos casamentos (ex: 'ibs', 'pis', 'aws').
    """
    palavras = term_n.split()
    partes = []
    for w in palavras:
        esc = re.escape(w)
        # plural opcional só pra palavras alfabéticas, 4+ letras, sem 's' final
        if w.isalpha() and len(w) >= 4 and not w.endswith('s'):
            esc += 's?'
        partes.append(esc)
    padrao = r'\b' + r'\s+'.join(partes) + r'\b'
    return re.compile(padrao)


def load_keywords():
    """Carrega os setores do keywords.yaml."""
    if not KEYWORDS_FILE.exists():
        sys.exit(f"Arquivo nao encontrado: {KEYWORDS_FILE}")
    try:
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        sys.exit(f"Erro no YAML (verifique indentacao): {e}")

    def _kw_normalize(text: str) -> str:
        nfkd = unicodedata.normalize("NFKD", (text or "").lower())
        return "".join(c for c in nfkd if not unicodedata.combining(c))

    sectors_raw = data.get("sectors", {}) or {}
    sectors = {}

    for sector_name, entries in sectors_raw.items():
        rules = []

        for item in entries:
            if isinstance(item, str):
                alias = item
                req_any = []
                req_none = []
                topic = None
            elif isinstance(item, dict) and "alias" in item:
                alias = item["alias"]
                req_any = item.get("requires_any") or []
                req_none = item.get("requires_none") or []
                topic = item.get("topic")
            else:
                sys.exit(f"Setor {sector_name}: entrada invalida {item}")

            alias_n = _kw_normalize(alias)
            rules.append({
                "alias": alias,
                "requires_any": req_any,
                "requires_none": req_none,
                "topic": topic,
                "_alias_re": re.compile(rf"\b{re.escape(alias_n)}\b"),
                "_requires_any_re": [
                    _compile_context_term(_kw_normalize(req))
                    for req in req_any
                ],
                "_requires_none_re": [
                    _compile_context_term(_kw_normalize(ban))
                    for ban in req_none
                ],
            })

        sectors[sector_name] = rules
        print(f"[sector] {sector_name}: {len(rules)} aliases", file=sys.stderr)

    return sectors


SECTORS = load_keywords()


# ============================================================
# FEEDS
# ============================================================

FEED_URLS = [
    # ----- Valor Econômico -----
    "https://pox.globo.com/rss/valor/empresas",
    "https://pox.globo.com/rss/valor/financas",
    "https://pox.globo.com/rss/valor/brasil",
    "https://pox.globo.com/rss/valor/legislacao",
    "https://pox.globo.com/rss/valor/politica",
    "https://pox.globo.com/rss/valor",
    "https://www.valor.com.br/rss",

    # ----- Folha de S.Paulo -----
    "https://feeds.folha.uol.com.br/mercado/rss091.xml",
    "https://feeds.folha.uol.com.br/folha/dinheiro/rss091.xml",
    "https://feeds.folha.uol.com.br/poder/rss091.xml",

    # ----- Estadão -----
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/economia/",
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/brasil/",
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/politica/",
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/negocios/",

    # ----- O Globo -----
    "https://pox.globo.com/rss/oglobo/economia",
    "https://pox.globo.com/rss/oglobo/politica",

    # ----- Pipeline Valor (M&A, negocios) -----
    "https://pox.globo.com/rss/pipelinevalor",
    "https://pox.globo.com/rss/pipelinevalor/ultimas",
    "https://pox.globo.com/rss/pipelinevalor/negocios",
    "https://news.google.com/rss/search?q=site:pipelinevalor.globo.com&hl=pt-BR&gl=BR&ceid=BR:pt-419",

    # ----- Outros portais brasileiros -----
    "https://exame.com/feed/",
    "https://veja.abril.com.br/feed",
    "https://veja.abril.com.br/economia/feed",
    "https://www.jota.info/feed",
    "https://mercadoeconsumo.com.br/feed/",
    "https://neofeed.com.br/feed/",
    "https://pox.globo.com/rss/epocanegocios",
    "https://forbes.com.br/feed/",
    "https://www.bloomberglinea.com.br/brasil/feed/",
    "https://www.bloomberglinea.com.br/feed/",
    "https://www.cnnbrasil.com.br/economia/feed/",

    # ----- Internacionais -----
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://www.forbes.com/business/feed/",
    "http://feeds.bbci.co.uk/news/business/rss.xml",
    "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best",
    "https://www.ft.com/rss/home",
    "https://www.ft.com/companies?format=rss",
    "https://feeds.content.dowjones.io/public/rss/RSSMarketsMain",
    "https://feeds.content.dowjones.io/public/rss/RSSWSJD",

    # ----- Google News BR: agregadores por veiculo -----
    "https://news.google.com/rss/search?q=site:valor.globo.com+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:oglobo.globo.com+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:folha.uol.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:estadao.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:exame.com+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:veja.abril.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:epocanegocios.globo.com+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:neofeed.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:braziljournal.com+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:bloomberglinea.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:forbes.com.br+when:2d&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:reuters.com+business&hl=en-US&gl=US&ceid=US:en",

    # ----- Google News: Saúde (GLP-1, canetas emagrecedoras) -----
    "https://news.google.com/rss/search?q=saude+site:oglobo.globo.com&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=saude+site:valor.globo.com&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=saude+site:folha.uol.com.br&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=saude+site:estadao.com.br&hl=pt-BR&gl=BR&ceid=BR:pt-419",
]

EXPERIMENTAL_FEEDS = [
    "https://pox.globo.com/rss/oglobo/negocios",
    # Ámbito e iProUP (Argentina) bloqueiam o feed direto com HTTP 403 a
    # partir dos IPs do runner (WAF regional). A via Google News site: é
    # servida pela infra do Google e contorna o bloqueio.
    "https://news.google.com/rss/search?q=site:ambito.com&hl=es-419&gl=AR&ceid=AR:es-419",
    "https://news.google.com/rss/search?q=site:iproup.com&hl=es-419&gl=AR&ceid=AR:es-419",
    "https://news.google.com/rss/search?q=site:eleconomista.com.mx&hl=es-419&gl=MX&ceid=MX:es-419",
    "https://news.google.com/rss/search?q=site:elfinanciero.com.mx&hl=es-419&gl=MX&ceid=MX:es-419",
    "https://redir.folha.com.br/redir/online/emcimadahora/rss091/*https://www1.folha.uol.com.br/emcimadahora/",
]

HTML_FALLBACK_PAGES = [
    "https://valor.globo.com/empresas/",
    "https://valor.globo.com/financas/",
    "https://valor.globo.com/legislacao/",
    "https://valor.globo.com/politica/",
    "https://pipelinevalor.globo.com/",
    "https://pipelinevalor.globo.com/negocios/",
    "https://www1.folha.uol.com.br/poder/",
    "https://www.estadao.com.br/politica/",
    "https://oglobo.globo.com/politica/",
    "https://www.gov.br/anvisa/pt-br/assuntos/noticias",
    "https://www.gov.br/receitafederal/pt-br/assuntos/noticias",
    "https://oglobo.globo.com/saude/",
    "https://valor.globo.com/saude/",
    "https://www1.folha.uol.com.br/equilibrioesaude/",
    "https://www.estadao.com.br/saude/",
]


# ============================================================
# Data model
# ============================================================

@dataclass
class Article:
    title: str
    summary: str
    url: str
    published: Optional[datetime] = None
    source: str = ""
    matched_sectors: List[str] = field(default_factory=list)
    matched_aliases: List[str] = field(default_factory=list)
    matches: List[dict] = field(default_factory=list)
    score: float = 0.0

    def to_json(self) -> dict:
        d = asdict(self)
        d["published"] = self.published.isoformat() if self.published else None
        return d


# ============================================================
# Helpers
# ============================================================

def normalize(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


# Cache em memoria pra evitar resolver a mesma URL 2x no mesmo run
_REDIRECT_CACHE: dict = {}

# User agent realista pro Google News (HEAD/GET genericos retornam 403)
_GOOGLE_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) "
              "Chrome/120.0.0.0 Safari/537.36")


def _decode_google_news_via_batchexecute(gn_art_id: str) -> Optional[str]:
    """
    Decodifica URL do Google News usando o endpoint batchexecute.

    Funciona em 2 passos:
    1. GET /articles/<id> pra extrair signature + timestamp do HTML
    2. POST /batchexecute com o payload garturlreq pra obter URL real

    Esse e o unico metodo confiavel desde julho/2024, quando o Google
    mudou pra IDs opacos (AU_yqL...) que nao contem a URL real no base64.

    Retorna None em caso de qualquer erro (403, parsing, timeout etc).
    """
    if not HAS_HTML or not gn_art_id:
        return None

    try:
        r = requests.get(
            f"https://news.google.com/rss/articles/{gn_art_id}",
            headers={"User-Agent": _GOOGLE_UA},
            timeout=8,
        )
        if r.status_code != 200:
            return None
        sig_match = re.search(r'data-n-a-sg="([^"]+)"', r.text)
        ts_match = re.search(r'data-n-a-ts="([^"]+)"', r.text)
        if not sig_match or not ts_match:
            return None
        signature = sig_match.group(1)
        timestamp = ts_match.group(1)
    except Exception:
        return None

    try:
        from urllib.parse import quote
        articles_req = [
            "Fbv4je",
            f'["garturlreq",[["X","X",["X","X"],null,null,1,1,'
            f'"US:en",null,1,null,null,null,null,null,0,1],'
            f'"X","X",1,[1,1,1],1,1,null,0,0,null,0],'
            f'"{gn_art_id}",{timestamp},"{signature}"]'
        ]
        payload = "f.req=" + quote(json.dumps([[articles_req]]))
        r = requests.post(
            "https://news.google.com/_/DotsSplashUi/data/batchexecute",
            headers={
                "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
                "User-Agent": _GOOGLE_UA,
            },
            data=payload,
            timeout=8,
        )
        if r.status_code != 200:
            return None
        parts = r.text.split("\n\n", 1)
        if len(parts) < 2:
            return None
        outer = json.loads(parts[1])
        for item in outer:
            if isinstance(item, list) and len(item) > 2 and item[2]:
                try:
                    inner = json.loads(item[2])
                    if (isinstance(inner, list) and len(inner) >= 2
                            and isinstance(inner[1], str)
                            and inner[1].startswith("http")):
                        return inner[1]
                except Exception:
                    continue
        return None
    except Exception:
        return None


def _resolve_redirect(url: str, max_hops: int = 3) -> Optional[str]:
    """
    Tenta resolver redirect:
    - Para Google News: usa batchexecute decoder (necessario desde 2024)
    - Para outras URLs: HEAD com follow_redirects (resolve a maioria)

    Retorna a URL final ou None em caso de erro/timeout.
    """
    if not HAS_HTML:
        return None
    if url in _REDIRECT_CACHE:
        return _REDIRECT_CACHE[url]

    final_url = None

    if 'news.google.com' in url:
        m = re.search(r'/articles/([^?/]+)', url)
        if m:
            gn_art_id = m.group(1)
            decoded = _decode_google_news_via_batchexecute(gn_art_id)
            if decoded:
                final_url = decoded
        _REDIRECT_CACHE[url] = final_url
        return final_url

    try:
        r = requests.head(url, headers={"User-Agent": USER_AGENT},
                          allow_redirects=True, timeout=8)
        if r.url and r.url != url:
            final_url = r.url
    except Exception:
        pass

    _REDIRECT_CACHE[url] = final_url
    return final_url


def clean_url(url: str) -> str:
    """
    Limpa a URL pra exibicao (essa e a URL salva no JSON e exibida no app).

    - Folha redir: extrai URL real apos '/rss091/*'
    - UOL: remove query string em paginas .htm
    - Google News: tenta resolver redirect HTTP pra extrair URL real
    - Outras: retorna sem trailing slash
    """
    if not url:
        return ""

    if '/rss091/*' in url:
        parts = url.split('/rss091/*', 1)
        if len(parts) == 2 and parts[1].startswith('http'):
            url = parts[1]

    if 'economia.uol.com.br' in url and '.htm' in url:
        url = url.split('?')[0]

    if 'news.google.com' in url:
        if 'url=' in url:
            try:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(url).query)
                if 'url' in q:
                    return q['url'][0].rstrip('/')
            except Exception:
                pass
        resolved = _resolve_redirect(url)
        if resolved and 'news.google.com' not in resolved:
            url = resolved

    return url.rstrip('/')


def dedup_key(url: str) -> str:
    """Chave usada apenas para deduplicacao."""
    cleaned = clean_url(url)
    if not cleaned:
        return ""
    if '?' in cleaned:
        from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
        try:
            parsed = urlparse(cleaned)
            q = [(k, v) for k, v in parse_qsl(parsed.query)
                 if not k.startswith('utm_') and k not in ('gclid', 'fbclid', '_ga')]
            cleaned = urlunparse(parsed._replace(query=urlencode(q)))
            cleaned = cleaned.rstrip('?').rstrip('/')
        except Exception:
            pass
    return cleaned


# Backwards-compat alias
normalize_url = dedup_key


def normalize_title_for_dedup(title: str) -> str:
    """Normaliza titulo para deduplicacao cross-source.

    Remove sufixos de veiculo no formato " - Veículo" pra que a mesma materia
    venha de fontes diferentes (RSS nativo + Google News) seja deduplicada.
    Sufixos compostos vem PRIMEIRO no regex (forbes brasil antes de forbes).
    """
    t = normalize(title or "")
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    venues_pattern = (
        r"forbes brasil|bloomberg linea brasil|bloomberg linea|"
        r"brazil journal|epoca negocios|folha de s paulo|folha de sao paulo|"
        r"valor economico|o globo|cnn brasil|estadao blue studio|"
        r"el economista|el financiero|"
        r"estadao|folha|valor|globo|veja|exame|"
        r"reuters|bloomberg|cnbc|forbes|ft|wsj|"
        r"neofeed|infomoney|poder360|jota|"
        r"pipelinevalor|pipeline|"
        r"o globo|o globo brasil"
    )
    for _ in range(3):
        new_t = re.sub(rf"\s+({venues_pattern})$", "", t)
        if new_t == t:
            break
        t = new_t
    return t.strip()


def is_junk_title(title: str) -> bool:
    """Rejeita titulos muito curtos ou genericos."""
    t = (title or "").strip().lower()
    if len(t) < 15:
        return True
    t_clean = re.sub(r"[^\w\s]", "", t).strip()
    return t_clean in JUNK_TITLES


# Heuristica leve de deteccao de ingles
_EN_INDICATORS = [
    r"\bthe\b", r"\band\b", r"\bof\b", r"\bto\b", r"\bin\b", r"\bfor\b",
    r"\bwith\b", r"\bfrom\b", r"\bthis\b", r"\bthat\b", r"\bthese\b",
    r"\bafter\b", r"\bbefore\b", r"\bhas\b", r"\bhave\b", r"\bwill\b",
    r"\bsays\b", r"\breports\b", r"\bamid\b", r"\bdespite\b",
    r"\bcompany\b", r"\bcompanies\b", r"\bsaid\b",
]
_EN_RE = re.compile("|".join(_EN_INDICATORS), re.IGNORECASE)

_PT_INDICATORS = [
    r"\bde\b", r"\bda\b", r"\bdo\b", r"\bno\b", r"\bna\b", r"\bdos\b",
    r"\bdas\b", r"\bnos\b", r"\bnas\b", r"\bque\b", r"\bcom\b",
    r"\bpara\b", r"\bpor\b", r"\bmas\b", r"\bseu\b", r"\bsua\b",
    r"\bos\b", r"\bas\b", r"\bum\b", r"\buma\b",
]
_PT_RE = re.compile("|".join(_PT_INDICATORS), re.IGNORECASE)


def is_english(text: str) -> bool:
    if not text:
        return False
    en_hits = len(_EN_RE.findall(text))
    pt_hits = len(_PT_RE.findall(text))
    return en_hits >= 3 and en_hits > pt_hits


def load_seen() -> set:
    if SEEN_DB.exists():
        try:
            return set(json.loads(SEEN_DB.read_text()))
        except Exception:
            return set()
    return set()


def save_seen(urls: set) -> None:
    SEEN_DB.write_text(json.dumps(list(urls), ensure_ascii=False))


# ============================================================
# Matching
# ============================================================

def _alias_matches(alias_re, scope_text: str,
                   requires_any_re: Optional[list],
                   requires_none_re: Optional[list],
                   full_text: str) -> bool:
    if not alias_re.search(scope_text):
        return False
    if requires_any_re and not any(req.search(full_text) for req in requires_any_re):
        return False
    if requires_none_re and any(ban.search(full_text) for ban in requires_none_re):
        return False
    return True


def _search_rule(scope_text: str, rule: dict, context_text: str) -> bool:
    """Aplica alias + requisitos dentro de um contexto controlado."""
    return _alias_matches(
        alias_re=rule["_alias_re"],
        scope_text=scope_text,
        requires_any_re=rule.get("_requires_any_re"),
        requires_none_re=rule.get("_requires_none_re"),
        full_text=context_text,
    )


def score_article(a: Article) -> None:
    """
    Score determinístico por setor.

    Mudanças principais:
    - Separa title, lead e body para reduzir falso positivo vindo de RSS longo.
    - Economia ignora body. O setor só entra se o termo aparecer no título
      ou nos primeiros LEAD_CHARS caracteres do summary.
    - Salva matches detalhados para auditoria e ajustes futuros.
    """
    a.matched_sectors = []
    a.matched_aliases = []
    a.matches = []
    a.score = 0.0

    title_n = normalize(a.title)
    summary_n = normalize(a.summary or "")
    lead_n = summary_n[:LEAD_CHARS]
    body_n = summary_n[LEAD_CHARS:LEAD_CHARS + BODY_CHARS]

    full_n = normalize(f"{a.title} {a.summary}")
    economy_context_n = normalize(f"{a.title} {summary_n[:LEAD_CHARS]}")

    matched_aliases = set()
    matched_sectors = []
    matches = []
    total_score = 0.0

    article_is_english = is_english(f"{a.title} {a.summary}")

    for sector_name, rules in SECTORS.items():
        if article_is_english and sector_name in BR_ONLY_SECTORS:
            continue

        sector_score = 0.0
        sector_has_match = False
        context_n = economy_context_n if sector_name == ECONOMY_SECTOR else full_n

        for rule in rules:
            if _search_rule(title_n, rule, context_n):
                sector_score += 20
                sector_has_match = True
                matched_aliases.add(rule["alias"])
                matches.append({
                    "sector": sector_name,
                    "alias": rule["alias"],
                    "field": "title",
                    "score": 20,
                    "topic": rule.get("topic"),
                })
                continue

            if _search_rule(lead_n, rule, context_n):
                sector_score += 8
                sector_has_match = True
                matched_aliases.add(rule["alias"])
                matches.append({
                    "sector": sector_name,
                    "alias": rule["alias"],
                    "field": "lead",
                    "score": 8,
                    "topic": rule.get("topic"),
                })
                continue

            if sector_name != ECONOMY_SECTOR and _search_rule(body_n, rule, context_n):
                sector_score += 2
                sector_has_match = True
                matched_aliases.add(rule["alias"])
                matches.append({
                    "sector": sector_name,
                    "alias": rule["alias"],
                    "field": "body",
                    "score": 2,
                    "topic": rule.get("topic"),
                })

        if sector_has_match:
            matched_sectors.append(sector_name)
            total_score += sector_score

    a.matched_sectors = matched_sectors
    a.matched_aliases = sorted(matched_aliases)
    a.matches = matches
    a.score = total_score


# ============================================================
# Topic caps
# ============================================================

def _parse_published_for_sort(a_dict: dict) -> float:
    try:
        published = a_dict.get("published")
        if not published:
            return 0.0
        return datetime.fromisoformat(published).timestamp()
    except Exception:
        return 0.0


def _article_topic_ids(a_dict: dict) -> List[str]:
    topics = []
    for m in a_dict.get("matches", []) or []:
        topic = m.get("topic")
        if topic and topic not in topics:
            topics.append(topic)
    return topics


def _bypass_topic_cap(a_dict: dict) -> bool:
    title_n = normalize(a_dict.get("title", ""))
    return bool(_TOPIC_CAP_BYPASS_RE.search(title_n))


def apply_topic_caps(articles: List[dict]) -> List[dict]:
    """
    Limita excesso de matérias de tópicos recorrentes.

    Regras:
    - Preserva matérias sem tópico.
    - Preserva matérias que citam empresa coberta no título.
    - Para artigos com tópico, mantém os de maior score e mais recentes.
    """
    if not TOPIC_CAPS:
        return articles

    ordered = sorted(
        articles,
        key=lambda x: (x.get("score", 0), _parse_published_for_sort(x)),
        reverse=True,
    )

    topic_counts = {topic: 0 for topic in TOPIC_CAPS}
    kept = []

    for a in ordered:
        topics = [t for t in _article_topic_ids(a) if t in TOPIC_CAPS]

        if not topics:
            kept.append(a)
            continue

        if _bypass_topic_cap(a):
            kept.append(a)
            continue

        if all(topic_counts[t] >= TOPIC_CAPS[t] for t in topics):
            continue

        kept.append(a)
        for t in topics:
            topic_counts[t] += 1

    return kept


# ============================================================
# Fetching
# ============================================================

def parse_entry(entry, source: str) -> Optional[Article]:
    try:
        title = (entry.get("title") or "").strip()
        url = (entry.get("link") or "").strip()
        url = clean_url(url)
        if is_blocked_url(url):
            return None
        raw_summary = entry.get("summary") or entry.get("description") or ""
        summary = re.sub(r"<[^>]+>", " ", raw_summary)
        summary = re.sub(r"\s+", " ", summary).strip()

        published = None
        for key in ("published_parsed", "updated_parsed"):
            tp = entry.get(key)
            if tp:
                published = datetime(*tp[:6], tzinfo=timezone.utc)
                break

        if not title or not url:
            return None
        if is_junk_title(title):
            return None
        return Article(title=title, summary=summary, url=url,
                       published=published, source=source)
    except Exception:
        return None


def fetch_rss(url: str) -> List[Article]:
    try:
        d = feedparser.parse(url, agent=USER_AGENT,
                             request_headers={'Cache-Control': 'no-cache'})
        if not d.entries:
            return []
        return [a for a in (parse_entry(e, url) for e in d.entries) if a]
    except Exception as e:
        print(f"  [feed fail] {url}: {e}", file=sys.stderr)
        return []


def fetch_html_fallback(url: str) -> List[Article]:
    if not HAS_HTML:
        return []
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=FEED_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        articles = []
        candidates = soup.select(
            "div.feed-post-body, article, div.bastian-feed-item, "
            "div[class*='feed-post'], div[class*='materia']"
        )
        seen_urls = set()
        for c in candidates:
            link_tag = c.find("a", href=True)
            if not link_tag:
                continue
            href = link_tag["href"]
            if not href.startswith("http"):
                continue
            if href in seen_urls:
                continue
            seen_urls.add(href)

            title_tag = c.find(["h2", "h3"]) or link_tag
            title = title_tag.get_text(" ", strip=True)
            summary_tag = c.find(class_=re.compile(r"(summary|resumo|subtitulo|deck)"))
            summary = summary_tag.get_text(" ", strip=True) if summary_tag else ""

            if title and len(title) > 15 and not is_junk_title(title):
                cleaned_href = clean_url(href)
                if is_blocked_url(cleaned_href):
                    continue
                articles.append(Article(
                    title=title, summary=summary, url=cleaned_href,
                    published=None, source=url,
                ))
        return articles
    except Exception as e:
        print(f"  [html fail] {url}: {e}", file=sys.stderr)
        return []


def dedup_articles(articles: List[Article]) -> List[Article]:
    by_url = {}
    for a in articles:
        nu = dedup_key(a.url)
        if nu and nu not in by_url:
            by_url[nu] = a
    articles = list(by_url.values())

    by_title = {}
    for a in articles:
        key = normalize_title_for_dedup(a.title)
        if not key:
            continue
        if key in by_title:
            existing = by_title[key]
            existing_is_gn = 'news.google.com' in existing.url
            new_is_gn = 'news.google.com' in a.url
            if existing_is_gn and not new_is_gn:
                by_title[key] = a
            elif not existing_is_gn and new_is_gn:
                pass
            elif a.published and (not existing.published or a.published > existing.published):
                by_title[key] = a
        else:
            by_title[key] = a
    return list(by_title.values())


# ============================================================
# Rendering
# ============================================================

def render_markdown(articles: List[Article]) -> str:
    today = datetime.now().strftime("%d/%m/%Y")
    out = [f"# Clipping - {today}\n"]
    out.append(f"_{len(articles)} noticias relevantes pra cobertura_\n")

    if not articles:
        out.append("\n_Nenhuma materia relevante no periodo._")
        return "\n".join(out)

    grouped = {}
    for a in articles:
        key = a.matched_sectors[0] if a.matched_sectors else "OUTROS"
        grouped.setdefault(key, []).append(a)

    for key in sorted(grouped.keys()):
        out.append(f"\n## {key}  _({len(grouped[key])})_\n")
        for a in grouped[key]:
            other_sectors = [s for s in a.matched_sectors if s != key]
            tags = ""
            if other_sectors:
                tags += " " + " ".join(f"_#{s}_" for s in other_sectors)
            date = a.published.astimezone().strftime("%d/%m %Hh%M") if a.published else ""
            summary_excerpt = a.summary[:220] + ("..." if len(a.summary) > 220 else "")
            out.append(f"- **[{a.title}]({a.url})**{tags}")
            if date or summary_excerpt:
                out.append(f"  _{date}_ - {summary_excerpt}")

    out.append(f"\n---\n_Gerado em {datetime.now().strftime('%H:%M')}_")
    return "\n".join(out)


def render_json(articles: List[Article]) -> str:
    return json.dumps([a.to_json() for a in articles], ensure_ascii=False, indent=2)


# ============================================================
# Run
# ============================================================

def run(since_hours: int, output_format: str, min_score: float,
        dry_run: bool, include_seen: bool) -> str:
    seen = set() if include_seen else load_seen()
    articles: List[Article] = []

    if dry_run:
        mocks = [
            ("Renner reporta alta de 8% nas vendas mesmas lojas do 3T",
             "Lojas Renner registrou crescimento de 8%..."),
            ("Natura avanca em plano de fusao",
             "A Natura &Co anunciou nova etapa..."),
        ]
        for t, s in mocks:
            articles.append(Article(title=t, summary=s, url=f"https://mock/{hash(t)}",
                                    published=datetime.now(timezone.utc), source="mock"))
    else:
        print(f"Buscando {len(FEED_URLS)} feeds principais...", file=sys.stderr)
        got_any_rss = False
        for url in FEED_URLS:
            items = fetch_rss(url)
            if items:
                got_any_rss = True
                print(f"  OK {url}: {len(items)} itens", file=sys.stderr)
                articles.extend(items)

        if not got_any_rss and HAS_HTML:
            print("Nenhum RSS funcionou. Tentando fallback HTML...", file=sys.stderr)
            for url in HTML_FALLBACK_PAGES:
                items = fetch_html_fallback(url)
                if items:
                    print(f"  OK {url}: {len(items)} itens", file=sys.stderr)
                    articles.extend(items)

    articles = dedup_articles(articles)

    for a in articles:
        score_article(a)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours) if since_hours else None
    filtered = [
        a for a in articles
        if a.score >= min_score
        and (not cutoff or not a.published or a.published >= cutoff)
        and dedup_key(a.url) not in seen
    ]
    filtered.sort(key=lambda a: (-a.score, -(a.published.timestamp() if a.published else 0)))

    if not dry_run and not include_seen:
        save_seen(seen | {dedup_key(a.url) for a in filtered})

    if output_format == "json":
        return render_json(filtered)
    return render_markdown(filtered)


def run_ci(output_path: str, since_hours: int = 48, keep_days: int = 7,
           rescore: bool = False, probe_experimental: bool = False) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    existing: List[dict] = []
    if path.exists():
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            existing = data.get("articles", [])
        except Exception:
            existing = []

    if rescore and existing:
        print(f"[rescore] Re-aplicando matching em {len(existing)} artigos...",
              file=sys.stderr)
        rescored = []
        dropped = 0
        for a_dict in existing:
            if is_junk_title(a_dict.get("title", "")):
                dropped += 1
                continue
            cleaned_existing_url = clean_url(a_dict.get("url", ""))
            if is_blocked_url(cleaned_existing_url):
                dropped += 1
                continue

            published = None
            if a_dict.get("published"):
                try:
                    published = datetime.fromisoformat(a_dict["published"])
                except Exception:
                    pass
            a = Article(
                title=a_dict.get("title", ""),
                summary=a_dict.get("summary", ""),
                url=cleaned_existing_url,
                published=published,
                source=a_dict.get("source", ""),
            )
            score_article(a)
            if a.score >= 1:
                rescored.append(a.to_json())
            else:
                dropped += 1
        existing = rescored
        print(f"[rescore] {len(rescored)} mantidos, {dropped} descartados",
              file=sys.stderr)

    existing_urls = {dedup_key(a.get("url", "")) for a in existing}
    existing_titles = {normalize_title_for_dedup(a.get("title", ""))
                       for a in existing if a.get("title")}

    feed_list = FEED_URLS[:]
    if probe_experimental:
        feed_list = feed_list + EXPERIMENTAL_FEEDS
        print(f"[probe] incluindo {len(EXPERIMENTAL_FEEDS)} feeds experimentais",
              file=sys.stderr)

    fetched: List[Article] = []
    print(f"Buscando {len(feed_list)} feeds...", file=sys.stderr)
    got_any_rss = False
    for url in feed_list:
        items = fetch_rss(url)
        if items:
            got_any_rss = True
            print(f"  OK RSS {url}: {len(items)} itens", file=sys.stderr)
            fetched.extend(items)

    if not got_any_rss and HAS_HTML:
        print("Nenhum RSS. Tentando HTML fallback...", file=sys.stderr)
        for url in HTML_FALLBACK_PAGES:
            items = fetch_html_fallback(url)
            if items:
                print(f"  OK HTML {url}: {len(items)} itens", file=sys.stderr)
                fetched.extend(items)

    fetched = dedup_articles(fetched)

    new_count = 0
    dup_count = 0
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    for a in fetched:
        nu = dedup_key(a.url)
        norm_title = normalize_title_for_dedup(a.title)

        if nu in existing_urls or (norm_title and norm_title in existing_titles):
            dup_count += 1
            continue

        if a.published and a.published < cutoff_time:
            continue

        score_article(a)
        if a.score >= 1:
            existing.append(a.to_json())
            existing_urls.add(nu)
            if norm_title:
                existing_titles.add(norm_title)
            new_count += 1

    keep_cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
    merged = [a for a in existing if (not a.get("published")) or a["published"] >= keep_cutoff]
    merged = apply_topic_caps(merged)
    merged.sort(key=lambda a: (-a.get("score", 0), a.get("published") or ""), reverse=False)

    path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(merged),
        "new_this_run": new_count,
        "articles": merged,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: {len(merged)} artigos no total ({new_count} novos, "
          f"{dup_count} duplicatas ignoradas) -> {output_path}",
          file=sys.stderr)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--since", type=int, default=24)
    p.add_argument("--format", choices=["markdown", "json"], default="markdown")
    p.add_argument("--min-score", type=float, default=1.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--include-seen", action="store_true")
    p.add_argument("--reset-seen", action="store_true")
    p.add_argument("--output-json", metavar="PATH")
    p.add_argument("--keep-days", type=int, default=7)
    p.add_argument("--rescore", action="store_true",
                   help="Re-aplica matching em todos os artigos existentes")
    p.add_argument("--probe-experimental", action="store_true",
                   help="Inclui feeds experimentais na busca")
    args = p.parse_args()

    if args.reset_seen:
        if SEEN_DB.exists():
            SEEN_DB.unlink()
        print("Historico zerado.", file=sys.stderr)
        return

    if args.output_json:
        run_ci(args.output_json, args.since, args.keep_days,
               args.rescore, args.probe_experimental)
        return

    print(run(args.since, args.format, args.min_score,
              args.dry_run, args.include_seen))


if __name__ == "__main__":
    main()
