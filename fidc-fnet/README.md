# fidc-fnet

Baixa os **Informes Mensais de FIDCs** publicados no [Fundos.NET](https://fnet.bmfbovespa.com.br/fnet/publico/abrirGerenciadorDocumentosCVM) (sistema da B3 usado para as entregas obrigatórias à CVM).

- CLI em Python, sem dependência além de `requests` e `PyYAML`
- Lista de fundos configurável em `fundos.yaml`
- Manifesto (`data/manifest.json`) com o que já foi baixado — reruns não rebaixam nada, mas **retificações** (mesmo id, versão maior) são pegas
- GitHub Actions roda todo mês e commita os arquivos novos

## Uso

```bash
pip install -r requirements.txt

python fnet_fidc.py                                  # mês anterior, fundos do fundos.yaml
python fnet_fidc.py --mes 2026-07                    # um mês específico
python fnet_fidc.py --desde 2026-01 --ate 2026-06    # carga histórica
python fnet_fidc.py --cnpj 12.345.678/0001-90 --mes 2026-07   # avulso, sem mexer no config
python fnet_fidc.py --mes 2026-07 --todos --dry-run  # varre todos os FIDCs do mês, sem baixar
```

Flags úteis: `--out` (pasta de destino), `--force` (rebaixa o que já está no manifesto), `--dry-run`, `-v`, `--intervalo` (pausa entre requisições, padrão 0,8 s), `--janela-meses`, `--tipo-fundo`.

## Configuração

`fundos.yaml`:

```yaml
tipo_fundo: 2        # no FNET: 1 = FII, 2 = FIDC, 4 = FIP
janela_meses: 3      # meses após a referência em que ainda buscamos entregas/retificações
padroes:
  - "informe mensal" # regex, sem acento e em minúsculo
fundos:
  - nome: "Fundo de Investimento em Direitos Creditórios Exemplo"
    apelido: "exemplo"     # vira o nome da pasta
    cnpj: "12.345.678/0001-90"
```

A lista vem vazia. Para descobrir CNPJs, rode `--todos --dry-run -v` no mês que interessa e copie os fundos que aparecerem.

## Onde os arquivos caem

```
data/
  2026/
    07/
      exemplo/
        2026-07_exemplo_1234567.xml        # id do documento no FNET
        2026-07_exemplo_1234567_v2.xml     # retificação (versão 2)
  manifest.json
```

Cada entrada do `manifest.json` guarda id, CNPJ, referência, versão, data de entrega, administrador, tamanho, `sha256` e o link de visualização no FNET.

## Como funciona (e o que pode quebrar)

O Fundos.NET não tem API pública documentada. O que existe é o endpoint que a própria tela de consulta chama:

| Endpoint | Para quê |
| --- | --- |
| `GET /fnet/publico/pesquisarGerenciadorDocumentosDados` | lista de documentos, em JSON no formato DataTables (`d`, `s`, `l`, `o[0][dataEntrega]`, `tipoFundo`, `cnpjFundo`, `dataInicial`, `dataFinal`, `situacao`) |
| `GET /fnet/publico/downloadDocumento?id=<id>` | o arquivo, normalmente em base64 |
| `GET /fnet/publico/exibirDocumento?id=<id>&cvm=true` | visualização no navegador (usado só como link no manifesto) |

Decisões de robustez, porque isso é API não-contratada:

- **Filtro no cliente, não no servidor.** Os ids de `idCategoriaDocumento` / `idTipoDocumento` mudam de tempos em tempos; a busca aqui é por CNPJ + janela de data de entrega e o "é informe mensal?" é decidido em cima do texto (`categoriaDocumento` + `tipoDocumento` + `especieDocumento`) que o próprio FNET devolve. Mais requisições, mas não quebra em renumeração.
- **Referência conferida documento a documento.** A janela de busca é por *data de entrega*; o que decide o mês é o `dataReferencia` (aceita `MM/AAAA` e `DD/MM/AAAA`).
- **CNPJ nas duas formas.** Tenta só dígitos e, se não voltar nada, tenta com máscara.
- **Conteúdo detectado, não presumido.** O corpo do download é decodificado de base64 quando for base64, e a extensão sai do `Content-Disposition` ou dos magic bytes (`.xml`, `.pdf`, `.zip`, `.json`, `.bin`).
- **Retry com backoff** (2s/4s/8s) em 5xx/429 e pausa entre requisições.

Se a B3 mudar os parâmetros de busca, o sintoma é "nenhum informe mensal encontrado" para todos os fundos — rode com `-v` para ver quantos documentos a busca trouxe antes do filtro.

## Testes

```bash
python -m unittest discover -s tests -v
```

São offline: o cliente HTTP recebe uma sessão falsa com respostas no formato do FNET, então nada aqui depende do site estar no ar.

---

> **Nota:** este diretório está hospedado temporariamente na branch
> `claude/fidc-fnet-bovespa-download-h4as7y` do repositório `clipping`, porque a
> sessão não tinha permissão para criar o repositório novo. O destino é o
> repositório próprio `vncsprttdsz/fidc-fnet`, com estes arquivos na raiz —
> aí os dois workflows em `.github/workflows/` passam a valer.
