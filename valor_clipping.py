#!/usr/bin/env python3
"""
Valor Economico - Clipping Diario
Puxa noticias do Valor, filtra pela cobertura de varejo/consumo do Itau BBA,
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
    import requests
    from bs4 import BeautifulSoup
    HAS_HTML = True
except ImportError:
    HAS_HTML = False


COVERAGE = {
    "LREN3":  ["renner", "lojas renner"],
    "CEAB3":  ["c&a", "ceab"],
    "NTCO3":  ["natura", "natura &co", "natura co", "avon"],
    "ASAI3":  ["assai"],
    "PGMN3":  ["pague menos", "extrafarma"],
    "PNVL3":  ["panvel", "dimed"],
    "ALLD3":  ["allied", "trocafy"],
    "SBFG3":  ["grupo sbf", "sbf", "centauro", "fila brasil", "nike brasil"],
    "MELI":
