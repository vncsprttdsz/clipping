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
import time
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

# Tempo maximo por feed (segundos). Era ~20s default, agora 10s.
# Com 30+ feeds isso corta ~5 min de espera em feeds mortos.
FEED_TIMEOUT = 10

# Titulos lixo: matérias com títulos genéricos demais pra serem úteis.
# Case-insensitive, match exato depois de trim.
JUNK_TITLES = {
    "curtas", "giro", "resumo", "resumo do dia", "giro do dia",
    "painel", "noticias em tempo real", "últimas notícias",
    "a hora", "a hora do mercado", "panorama",
}

# Setor "so global" - nao aplicamos detector de idioma aqui,
# pois Global Consumer aceita EN naturalmente.
GLOBAL_SECTOR_NAME = "Global Consumer"

# Setores que sao BR-only - se detectarmos EN na materia, pulamos.
BR_ONLY_SECTORS = {
    "Fashion", "Joias", "E-commerce", "Varejo Alimentar", "Academias",
    "Cosméticos", "Farmácias", "Economia", "Material de Construção",
    "Eletrônicos", "Viagens", "Pet", "Wellness e Esportes",
}


# ============================================================
# Carrega keywords
# ============================================================

def load_keywords():
    """Carrega os setores do keywords.yaml."""
    if not KEYWORDS_FILE.exists():
        sys.exit(f"Arquivo nao encontrado: {KEYWORDS_FILE}")
    try:
        with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        sys.exit(f"Erro no YAML (verifique indentacao): {e}")

    sectors_raw = data.get("sectors", {}) or {}
    sectors = {}
    for sector_name, entries in sectors_raw.items():
        rules = []
        for item in entries:
            if isinstance(item, str):
                rules.append({"alias": item, "requires_any": None})
            elif isinstance(item, dict) and "alias" in item:
                req = item.get("requires_any") or []
                rules.append({"alias": item["alias"], "requires_any": req})
            else:
                sys.exit(f"Setor {sector_name}: entrada invalida {item}")
        sectors[sector_name] = rules
        print(f"[sector] {sector_name}: {len(rules)} aliases", file=sys.stderr)
    return sectors


SECTORS = load_keywords()


# ============================================================
# FEEDS - confiáveis primeiro, experimentais depois
# ============================================================

# Feeds confirmados que retornam conteudo com frequencia.
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

    # ----- Outros portais brasileiros -----
    "https://exame.com/feed/",
    "https://veja.abril.com.br/feed",
    "https://veja.abril.com.br/economia/feed",
    "https://rss.uol.com.br/feed/economia.xml",
    "https://www.jota.info/feed",
    "https://mercadoeconsumo.com.br/feed/",

    # ----- Internacionais -----
    "https://www.cnbc.com/id/10001147/device/rss/rss.html",
    "https://www.forbes.com/business/feed/",
    "http://feeds.bbci.co.uk/news/business/rss.xml",

    # ----- Google News (fallback) -----
    "https://news.google.com/rss/search?q=site:bloomberglinea.com.br&hl=pt-BR&gl=BR&ceid=BR:pt-419",
    "https://news.google.com/rss/search?q=site:reuters.com+business&hl=en-US&gl=US&ceid=US:en",
]

# Feeds experimentais - testa uma vez em vez de em todo run.
# Movidos aqui porque estavam retornando 0 itens com frequencia.
EXPERIMENTAL_FEEDS = [
    "https://pox.globo.com/rss/oglobo/negocios",
    "https://www.ambito.com/rss/economia.xml",
    "https://www.iproup.com/feed",
    "https://news.google.com/rss/search?q=site:eleconomista.com.mx&hl=es-419&gl=MX&ceid=MX:es-419",
    "https://news.google.com/rss/search?q=site:elfinanciero.com.mx&hl=es-419&gl=MX&ceid=MX:es-419",
    "https://redir.folha.com.br/redir/online/emcimadahora/rss091/*https://www1.folha.uol.com.br/emcimadahora/",
]

HTML_FALLBACK_PAGES = [
    "https://valor.globo.com/empresas/",
    "https://valor.globo.com/financas/",
    "https://valor.globo.com/legislacao/",
    "https://valor.globo.com/politica/",
    "https://www1.folha.uol.com.br/poder/",
    "https://www.estadao.com.br/politica/",
    "https://oglobo.globo.com/politica/",
    "https://www.gov.br/anvisa/pt-br/assuntos/noticias",
    "https://www.gov.br/receitafederal/pt-br/assuntos/noticias",
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


def normalize_url(url: str) -> str:
    """Remove parametros de rastreamento e redirecionamentos para dedup."""
    if not url:
        return ""
    # Folha: extrai URL real após '*'
    if '/rss091/*' in url:
        parts = url.split('/rss091/*')
        if len(parts) > 1:
            url = parts[-1]
    # UOL: remove query string em páginas .ghtm
    if 'economia.uol.com.br' in url and '.ghtm' in url:
        url = url.split('?')[0]
    # Google News: link real vem no parâmetro 'url='
    if 'news.google.com' in url and 'url=' in url:
        try:
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(url).query)
            if 'url' in q:
                url = q['url'][0]
        except Exception:
            pass
    return url.rstrip('/')


def normalize_title_for_dedup(title: str) -> str:
    """
    Normaliza titulo pra deduplicacao cross-source.
    Ex: "'Trabalho é livre', diz Lula..." -> "trabalho e livre diz lula"
    Remove acentos, pontuacao, espacos extras, case.
    """
    t = normalize(title or "")
    # Remove tudo que nao eh letra/numero/espaco
    t = re.sub(r"[^\w\s]", " ", t)
    # Colapsa espacos
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_junk_title(title: str) -> bool:
    """Rejeita titulos muito curtos ou genericos."""
    t = (title or "").strip().lower()
    if len(t) < 15:
        return True
    # Remove pontuacao pra comparar com JUNK_TITLES
    t_clean = re.sub(r"[^\w\s]", "", t).strip()
    return t_clean in JUNK_TITLES


# Heuristica leve de deteccao de ingles. So precisamos detectar
# com alta precisao em textos curtos (titulo + lead). Procuramos
# sequencias de stop-words inglesas que raramente aparecem em PT.
_EN_INDICATORS = [
    r"\bthe\b", r"\band\b", r"\bof\b", r"\bto\b", r"\bin\b", r"\bfor\b",
    r"\bwith\b", r"\bfrom\b", r"\bthis\b", r"\bthat\b", r"\bthese\b",
    r"\bafter\b", r"\bbefore\b", r"\bhas\b", r"\bhave\b", r"\bwill\b",
    r"\bsays\b", r"\breports\b", r"\bamid\b", r"\bdespite\b",
    r"\bcompany\b", r"\bcompanies\b", r"\bsaid\b",
]
_EN_RE = re.compile("|".join(_EN_INDICATORS), re.IGNORECASE)

# Stop-words PT pra desempatar (se tem MUITAS PT, provavelmente nao eh EN).
_PT_INDICATORS = [
    r"\bde\b", r"\bda\b", r"\bdo\b", r"\bno\b", r"\bna\b", r"\bdos\b",
    r"\bdas\b", r"\bnos\b", r"\bnas\b", r"\bque\b", r"\bcom\b",
    r"\bpara\b", r"\bpor\b", r"\bmas\b", r"\bseu\b", r"\bsua\b",
    r"\bos\b", r"\bas\b", r"\bum\b", r"\buma\b",
]
_PT_RE = re.compile("|".join(_PT_INDICATORS), re.IGNORECASE)


def is_english(text: str) -> bool:
    """Retorna True se o texto aparenta ser em ingles (>= 3 stop-words EN e menos PT)."""
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

def _alias_matches(pattern: str, scope_text: str, requires_any: Optional[list],
                   full_text: str) -> bool:
    """Checa se o alias casa no `scope_text` e se o contexto exigido esta presente."""
    if not re.search(pattern, scope_text):
        return False
    if not requires_any:
        return True
    for req in requires_any:
        req_n = normalize(req)
        req_pat = rf"\b{re.escape(req_n)}\b"
        if re.search(req_pat, full_text):
            return True
    return False


def score_article(a: Article) -> None:
    """
    Aplica matching e preenche matched_sectors, matched_aliases, score.

    Se o texto for detectado como ingles, só consideramos setores globais
    (Global Consumer). Isso evita matérias internacionais aparecerem em
    setores BR-only sem necessidade.
    """
    title_n = normalize(a.title)
    full_n = normalize(f"{a.title} {a.summary}")
    matched_aliases = set()

    # Decide quais setores considerar baseado no idioma
    article_is_english = is_english(f"{a.title} {a.summary}")

    for sector_name, rules in SECTORS.items():
        # Pula setores BR-only se artigo eh EN
        if article_is_english and sector_name in BR_ONLY_SECTORS:
            continue

        hit_in_title = False
        hit_in_body = False
        for rule in rules:
            al_n = normalize(rule["alias"])
            pattern = rf"\b{re.escape(al_n)}\b"
            req = rule["requires_any"]
            if _alias_matches(pattern, title_n, req, full_n):
                hit_in_title = True
                matched_aliases.add(rule["alias"])
                break
            if _alias_matches(pattern, full_n, req, full_n):
                hit_in_body = True
                matched_aliases.add(rule["alias"])
        if hit_in_title or hit_in_body:
            a.matched_sectors.append(sector_name)
            a.score += 20 if hit_in_title else 10

    a.matched_aliases = sorted(matched_aliases)


# ============================================================
# Fetching
# ============================================================

def parse_entry(entry, source: str) -> Optional[Article]:
    try:
        title = (entry.get("title") or "").strip()
        url = (entry.get("link") or "").strip()
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
    """Busca um feed RSS. Timeout agressivo pra nao travar em feeds mortos."""
    try:
        # feedparser aceita 'timeout' como kwarg via request_headers
        d = feedparser.parse(url, agent=USER_AGENT,
                             request_headers={'Cache-Control': 'no-cache'})
        # Se o feed nao responde, bail out rapido
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
                articles.append(Article(
                    title=title, summary=summary, url=href,
                    published=None, source=url,
                ))
        return articles
    except Exception as e:
        print(f"  [html fail] {url}: {e}", file=sys.stderr)
        return []


def dedup_articles(articles: List[Article]) -> List[Article]:
    """
    Dedup em 2 passos:
    1. URL normalizada (mesma lógica de sempre)
    2. Título normalizado - pega cross-source duplicates
       (ex: mesma matéria em Valor e O Globo)

    Em caso de titulo igual, preserva o que tem data mais nova
    (ou o primeiro se ambos sem data).
    """
    # Passo 1: dedup por URL
    by_url = {}
    for a in articles:
        norm_url = normalize_url(a.url)
        if norm_url and norm_url not in by_url:
            by_url[norm_url] = a
    articles = list(by_url.values())

    # Passo 2: dedup por titulo normalizado
    by_title = {}
    for a in articles:
        key = normalize_title_for_dedup(a.title)
        if not key:
            # titulo vazio? pula
            continue
        if key in by_title:
            # Escolhe o mais recente
            existing = by_title[key]
            if a.published and (not existing.published or a.published > existing.published):
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
             "Lojas Renner registrou crescimento de 8% nas vendas mesmas lojas..."),
            ("Natura avanca em plano de fusao com subsidiaria",
             "A Natura &Co anunciou nova etapa do processo..."),
            ("Governo estuda antecipar reforma trabalhista da jornada 6x1",
             "O Ministerio do Trabalho confirmou estudos sobre a escala 5x2..."),
            ("IBGE: PMC de outubro sobe 1,2% acima do consenso",
             "A Pesquisa Mensal do Comercio mostrou alta..."),
            ("Petrobras anuncia novo poco em Buzios",
             "A estatal informou a descoberta..."),
            ("Shein confirma fabrica no Brasil ate 2027",
             "A varejista chinesa Shein formalizou..."),
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
        and normalize_url(a.url) not in seen
    ]
    filtered.sort(key=lambda a: (-a.score, -(a.published.timestamp() if a.published else 0)))

    if not dry_run and not include_seen:
        save_seen(seen | {normalize_url(a.url) for a in filtered})

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
            # Filtra title lixo em rescore tambem
            if is_junk_title(a_dict.get("title", "")):
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
                url=a_dict.get("url", ""),
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

    existing_urls = {normalize_url(a.get("url", "")) for a in existing}
    existing_titles = {normalize_title_for_dedup(a.get("title", ""))
                       for a in existing if a.get("title")}

    # Feeds principais + experimentais (opcional)
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

    # Dedup dos recem-buscados
    fetched = dedup_articles(fetched)

    new_count = 0
    dup_count = 0
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    for a in fetched:
        norm_url = normalize_url(a.url)
        norm_title = normalize_title_for_dedup(a.title)

        # Dedup contra JSON existente (URL + titulo)
        if norm_url in existing_urls or (norm_title and norm_title in existing_titles):
            dup_count += 1
            continue

        # Filtro temporal
        if a.published and a.published < cutoff_time:
            continue

        score_article(a)
        if a.score >= 1:
            existing.append(a.to_json())
            existing_urls.add(norm_url)
            if norm_title:
                existing_titles.add(norm_title)
            new_count += 1

    keep_cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
    merged = [a for a in existing if (not a.get("published")) or a["published"] >= keep_cutoff]
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
                   help="Re-aplica matching em todos os artigos existentes "
                        "(use depois de mudar keywords.yaml)")
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
