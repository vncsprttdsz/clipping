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
# Carrega keywords do arquivo YAML (nova estrutura por setores)
# ============================================================

KEYWORDS_FILE = Path(__file__).parent / "keywords.yaml"


def load_keywords():
    """Carrega os setores do novo keywords.yaml."""
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
# FEEDS
# ============================================================

FEED_URLS = [
    # Valor Economico
    "https://pox.globo.com/rss/valor/empresas",
    "https://pox.globo.com/rss/valor/financas",
    "https://pox.globo.com/rss/valor/brasil",
    "https://pox.globo.com/rss/valor",
    "https://www.valor.com.br/rss",
    # Folha de S.Paulo
    "https://feeds.folha.uol.com.br/mercado/rss091.xml",
    "https://feeds.folha.uol.com.br/folha/dinheiro/rss091.xml",
    # Estadao (padrao Arc Publishing)
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/economia/",
    "https://www.estadao.com.br/arc/outboundfeeds/feeds/rss/sections/brasil/",
    # O Globo
    "https://pox.globo.com/rss/oglobo/economia",
    # Exame
    "https://exame.com/feed/",
    # Veja
    "https://veja.abril.com.br/feed",
    "https://veja.abril.com.br/economia/feed",
    # UOL Economia
    "https://rss.uol.com.br/feed/economia.xml",
]

HTML_FALLBACK_PAGES = [
    "https://valor.globo.com/empresas/",
    "https://valor.globo.com/financas/",
]

SEEN_DB = Path.home() / ".valor_clipping_seen.json"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"


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


def normalize(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", (text or "").lower())
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def load_seen() -> set:
    if SEEN_DB.exists():
        try:
            return set(json.loads(SEEN_DB.read_text()))
        except Exception:
            return set()
    return set()


def save_seen(urls: set) -> None:
    SEEN_DB.write_text(json.dumps(list(urls), ensure_ascii=False))


def _alias_matches(pattern: str, scope_text: str, requires_any: Optional[list],
                   full_text: str) -> bool:
    """Checa se o alias casa no `scope_text` e se o contexto exigido
    (palavra adicional em qualquer lugar do full_text) esta presente."""
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
    title_n = normalize(a.title)
    full_n = normalize(f"{a.title} {a.summary}")
    matched_aliases = set()

    for sector_name, rules in SECTORS.items():
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
        return Article(title=title, summary=summary, url=url,
                       published=published, source=source)
    except Exception:
        return None


def fetch_rss(url: str) -> List[Article]:
    try:
        d = feedparser.parse(url, agent=USER_AGENT)
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
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=15)
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

            if title and len(title) > 15:
                articles.append(Article(
                    title=title, summary=summary, url=href,
                    published=None, source=url,
                ))
        return articles
    except Exception as e:
        print(f"  [html fail] {url}: {e}", file=sys.stderr)
        return []


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
        print(f"Buscando {len(FEED_URLS)} feeds candidatos...", file=sys.stderr)
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

    by_url = {}
    for a in articles:
        if a.url not in by_url:
            by_url[a.url] = a
    articles = list(by_url.values())

    for a in articles:
        score_article(a)

    cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours) if since_hours else None
    filtered = [
        a for a in articles
        if a.score >= min_score
        and (not cutoff or not a.published or a.published >= cutoff)
        and a.url not in seen
    ]
    filtered.sort(key=lambda a: (-a.score, -(a.published.timestamp() if a.published else 0)))

    if not dry_run and not include_seen:
        save_seen(seen | {a.url for a in filtered})

    if output_format == "json":
        return render_json(filtered)
    return render_markdown(filtered)


def run_ci(output_path: str, since_hours: int = 48, keep_days: int = 7,
           rescore: bool = False) -> None:
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
        print(f"[rescore] Re-aplicando matching em {len(existing)} artigos existentes...",
              file=sys.stderr)
        rescored = []
        dropped = 0
        for a_dict in existing:
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

    existing_urls = {a.get("url") for a in existing}

    fetched: List[Article] = []
    print(f"Buscando {len(FEED_URLS)} feeds...", file=sys.stderr)
    got_any_rss = False
    for url in FEED_URLS:
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

    new_count = 0
    for a in fetched:
        if a.url in existing_urls:
            continue
        score_article(a)
        if a.score >= 1:
            existing.append(a.to_json())
            existing_urls.add(a.url)
            new_count += 1

    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat()
    merged = [a for a in existing if (not a.get("published")) or a["published"] >= cutoff]
    merged.sort(key=lambda a: (-a.get("score", 0), a.get("published") or ""), reverse=False)

    path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(merged),
        "new_this_run": new_count,
        "articles": merged,
    }, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"OK: {len(merged)} artigos no total ({new_count} novos) -> {output_path}",
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
    args = p.parse_args()

    if args.reset_seen:
        if SEEN_DB.exists():
            SEEN_DB.unlink()
        print("Historico zerado.", file=sys.stderr)
        return

    if args.output_json:
        run_ci(args.output_json, args.since, args.keep_days, args.rescore)
        return

    print(run(args.since, args.format, args.min_score,
              args.dry_run, args.include_seen))


if __name__ == "__main__":
    main()
