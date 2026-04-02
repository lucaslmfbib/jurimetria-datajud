# Guia Pratico de Codigos por Categoria de Tribunal

Este guia foi reorganizado para a pergunta pratica de uso:

"Se eu estiver no `TJMMG`, `TRF1`, `TJMG`, `TRT3` ou em um tribunal superior, quais codigos fazem mais sentido para eu testar?"

Importante:

- dentro do mesmo segmento, os tribunais costumam compartilhar a mesma base de classes;
- a excecao e a categoria `Tribunais Superiores e Conselhos`, porque `STJ`, `TST`, `TSE`, `STM`, `CJF`, `CSJT` e `CNJ` nao usam exatamente a mesma base;
- as listas abaixo sao **listas basicas para teste/analise**, nao a tabela completa de cada segmento.

## Como usar no app

1. escolha a sigla correta do tribunal;
2. veja a categoria do tribunal;
3. use um dos codigos-base daquela categoria;
4. se estiver buscando por `Numero do processo`, o principal continua sendo acertar a sigla do tribunal.

## 1) Tribunal de Justica Militar de Minas Gerais (TJMMG)

No app, a sigla e `tjmmg`.

Se voce quer uma lista basica para testar no `TJMMG`, estes sao os codigos mais seguros para comecar:

| Codigo | Classe | Quando faz sentido |
|---|---|---|
| `11041` | Inquerito Policial Militar | Um dos testes mais tipicos da Justica Militar |
| `11030` | Processo Criminal - Militar | Classe-base militar muito recorrente |
| `279` | Inquerito Policial | Classe habilitada para toda a Justica Militar Estadual |
| `11955` | Cautelar Inominada Criminal | Habilitada para toda a Justica Militar Estadual |
| `325` | Conflito de Jurisdicao | Util para testes no 2o grau/TJM |
| `15423` | Revisao Judicial - Conselho de Justificacao | Classe criada e habilitada para toda a Justica Militar Estadual |
| `120` | Mandado de Seguranca Civel | Tambem aparece no escopo militar |

Se quiser ampliar a amostra do `TJMMG`, tambem vale testar:

- `283` = Acao Penal - Procedimento Ordinario
- `10943` = Acao Penal - Procedimento Sumario
- `10944` = Acao Penal - Procedimento Sumarissimo
- `308` = Medidas Cautelares
- `303` = Medidas Garantidoras
- `327` = Embargos de Terceiro

## 2) Tribunais de Justica Militar

Esta mesma base inicial costuma servir para:

- `TJMMG`
- `TJMRS`
- `TJMSP`

Lista basica:

| Codigo | Classe |
|---|---|
| `11041` | Inquerito Policial Militar |
| `11030` | Processo Criminal - Militar |
| `279` | Inquerito Policial |
| `11955` | Cautelar Inominada Criminal |
| `325` | Conflito de Jurisdicao |
| `15423` | Revisao Judicial - Conselho de Justificacao |
| `120` | Mandado de Seguranca Civel |

## 3) Tribunais Regionais do Trabalho

Esta base costuma servir para:

- `TRT1` ate `TRT24`

Lista basica:

| Codigo | Classe |
|---|---|
| `985` | Acao Trabalhista - Rito Ordinario |
| `1125` | Acao Trabalhista - Rito Sumarissimo |
| `1126` | Acao Trabalhista - Rito Sumario (Alcada) |
| `980` | Acao de Cumprimento |
| `986` | Inquerito para Apuracao de Falta Grave |
| `112` | Homologacao de Transacao Extrajudicial |
| `987` | Dissidio Coletivo |
| `988` | Dissidio Coletivo de Greve |
| `1202` | Reclamacao |
| `1009` | Recurso Ordinario Trabalhista |

## 4) Tribunais de Justica

Esta base costuma servir para:

- `TJMG`
- `TJSP`
- `TJRJ`
- `TJRS`
- `TJPR`
- e demais `TJ*`

Lista basica para teste civel/juizados:

| Codigo | Classe |
|---|---|
| `436` | Procedimento do Juizado Especial Civel |
| `14695` | Procedimento do Juizado Especial da Fazenda Publica |
| `156` | Cumprimento de Sentenca |
| `12154` | Execucao de Titulo Extrajudicial |
| `12079` | Execucao de Titulo Extrajudicial contra a Fazenda Publica |
| `1116` | Execucao Fiscal |

Lista basica para teste recursal e de controle:

| Codigo | Classe |
|---|---|
| `198` | Apelacao |
| `202` | Agravo de Instrumento |
| `199` | Reexame Necessario |
| `1728` | Apelacao / Reexame Necessario |
| `1690` | Acao Civil Publica |
| `1691` | Mandado de Seguranca |
| `64` | Acao Civil de Improbidade Administrativa |

Observacao importante:

- o codigo `159` aparece em orientacoes e glossarios do CNJ como ramo de `Execucao de Titulo Extrajudicial`, mas a orientacao mais segura para teste e preferir as classes-folha `12154`, `12079` e `12447`, em vez de usar `159` sozinho.

## 5) Tribunais Regionais Federais

Esta base costuma servir para:

- `TRF1`
- `TRF2`
- `TRF3`
- `TRF4`
- `TRF5`
- `TRF6`

Lista basica civel/fazenda:

| Codigo | Classe |
|---|---|
| `156` | Cumprimento de Sentenca |
| `12154` | Execucao de Titulo Extrajudicial |
| `12079` | Execucao de Titulo Extrajudicial contra a Fazenda Publica |
| `1116` | Execucao Fiscal |
| `198` | Apelacao |
| `202` | Agravo de Instrumento |
| `199` | Reexame Necessario |
| `1728` | Apelacao / Reexame Necessario |
| `64` | Acao Civil de Improbidade Administrativa |

Lista basica criminal:

| Codigo | Classe |
|---|---|
| `283` | Acao Penal - Procedimento Ordinario |
| `10943` | Acao Penal - Procedimento Sumario |
| `10944` | Acao Penal - Procedimento Sumarissimo |
| `308` | Medidas Cautelares |
| `303` | Medidas Garantidoras |
| `1710` | Mandado de Seguranca |
| `293` | Crimes Ambientais |

## 6) Tribunais Superiores e Conselhos

Aqui a regra muda:

- `STJ`, `TST`, `TSE` e `STM` nao compartilham exatamente a mesma base;
- `CJF`, `CSJT` e `CNJ` tem forte carga administrativa/disciplinar;
- na API publica do DataJud, a pagina oficial de endpoints lista `STJ`, `TST`, `TSE` e `STM`, mas nao lista `CNJ`, `CJF` e `CSJT` como endpoints publicos de pesquisa processual.

### 6.1 STJ

Lista basica:

| Codigo | Classe |
|---|---|
| `15228` | Queixa-Crime |
| `11881` | Agravo em Recurso Especial |
| `1031` | Recurso Ordinario |
| `1044` | Agravo de Instrumento |
| `1670` | Acao de Improbidade Administrativa |

### 6.2 TST

Lista basica:

| Codigo | Classe |
|---|---|
| `1008` | Recurso de Revista |
| `1002` | Agravo de Instrumento em Recurso de Revista |
| `11882` | Recurso de Revista com Agravo |
| `1009` | Recurso Ordinario Trabalhista |
| `1004` | Agravo de Peticao |
| `987` | Dissidio Coletivo |
| `988` | Dissidio Coletivo de Greve |
| `980` | Acao de Cumprimento |
| `1202` | Reclamacao |
| `976` | Acao Anulatoria de Clausulas Convencionais |

### 6.3 TSE

Lista basica:

| Codigo | Classe |
|---|---|
| `11525` | Processos Civeis-Eleitorais |
| `11526` | Acao de Impugnacao de Mandato Eletivo |
| `11527` | Acao de Investigacao Judicial Eleitoral |
| `11528` | Acao Penal Eleitoral |
| `11533` | Recurso Contra Expedicao de Diploma |
| `11541` | Representacao |
| `11549` | Recurso Especial Eleitoral |
| `11550` | Recurso Ordinario |

### 6.4 STM

O `STM` compartilha a base militar. Para teste inicial, use:

| Codigo | Classe |
|---|---|
| `11041` | Inquerito Policial Militar |
| `11030` | Processo Criminal - Militar |
| `279` | Inquerito Policial |
| `11955` | Cautelar Inominada Criminal |
| `325` | Conflito de Jurisdicao |
| `15423` | Revisao Judicial - Conselho de Justificacao |
| `120` | Mandado de Seguranca Civel |

### 6.5 Conselhos: CJF, CSJT e CNJ

Para `CJF`, `CSJT` e `CNJ`, o mais comum e encontrar classes administrativas e disciplinares, nao o mesmo perfil de classe judicial dos tribunais.

Exemplos uteis, principalmente no `CSJT`, em material oficial recente do CNJ:

| Codigo | Classe |
|---|---|
| `11887` | Acompanhamento de Cumprimento de Decisao |
| `1298` | Processo Administrativo |
| `1299` | Recurso Administrativo |
| `1308` | Sindicancia |
| `11892` | Revisao Disciplinar |
| `1301` | Reclamacao Disciplinar |
| `1264` | Processo Administrativo em Face de Magistrado |
| `1262` | Processo Administrativo em Face de Servidor |
| `1306` | Recurso em Processo Administrativo Disciplinar em Face de Servidor |
| `88` | Correcao Parcial ou Reclamacao Correicional |

Observacao pratica:

- essas classes fazem sentido para consulta em TPU/SGT;
- mas, para o app baseado na API publica do DataJud, voce deve primeiro confirmar se existe endpoint publico para o orgao, porque `CNJ`, `CJF` e `CSJT` nao aparecem na lista publica de endpoints.

## 7) Regra curta para decidir rapido

Se quiser decidir rapido sem ler tudo:

- `TJMMG`, `TJMRS`, `TJMSP` -> comece com `11041`, `11030`, `279`
- `TJMG`, `TJSP`, `TJRJ`, `TJRS` -> comece com `436`, `14695`, `156`, `1116`
- `TRF1` a `TRF6` -> comece com `156`, `1116`, `198`, `202`
- `TRT1` a `TRT24` -> comece com `985`, `1125`, `980`, `987`
- `STJ` -> comece com `11881`, `15228`
- `TST` -> comece com `1008`, `1002`, `987`
- `TSE` -> comece com `11527`, `11528`, `11541`
- `STM` -> comece com `11041`, `11030`, `325`

## 8) Fontes oficiais usadas

- API Publica do DataJud - endpoints oficiais:
  <https://datajud-wiki.cnj.jus.br/api-publica/endpoints/>
- Consulta publica de classes do CNJ:
  <https://www.cnj.jus.br/sgt/consulta_publica_classes.php>
- Tabela de classes da Justica do Trabalho:
  <https://www.cnj.jus.br/wp-content/uploads/2011/02/tabela_de_classes_da_justia_do_trabalho.pdf>
- Tabela de classes da Justica Eleitoral:
  <https://www.cnj.jus.br/wp-content/uploads/2011/02/tabela-de-classes-justia-eleitoral.pdf>
- Guia de aplicacao da TTDU:
  <https://www.cnj.jus.br/wp-content/uploads/2023/12/guia-de-aplicacao-da-tabela-de-temporalidade-v3-2023-12-07-atualizado2.pdf>
- Boletim TPU de 10/04/2024:
  <https://www.cnj.jus.br/wp-content/uploads/2024/05/boletim-das-atualizacoes-tabelas-processuais-unificadas-2024-04-10-v2-2024-04-24.pdf>
- Boletim TPU de 29/11/2024:
  <https://www.cnj.jus.br/wp-content/uploads/2024/12/boletim-das-atualizacoes-tabelas-processuais-unificadas-29-11-2024.pdf>
- Glossarios do CNJ para metas/segmentos:
  <https://www.cnj.jus.br/wp-content/uploads/2023/01/glossario-metas-nacionais-do-poder-judiciario-2022-justica-do-trabalho-versao-3.pdf>
  <https://www.cnj.jus.br/wp-content/uploads/2021/03/Gloss%C3%A1rio-Metas-Nacionais-do-Poder-Judici%C3%A1rio-2021-Justi%C3%A7a-Federal-Vers%C3%A3o-2.pdf>
  <https://www.cnj.jus.br/wp-content/uploads/2022/05/glossario-metas-nacionais-do-poder-judiciario-2022-justica-estadual-versao-3-1.pdf>
  <https://www.cnj.jus.br/wp-content/uploads/2024/02/glossario-metas-nacionais-do-poder-judiciario-2023-jmu-e-jme-versao-5.pdf>
  <https://www.cnj.jus.br/wp-content/uploads/2018/04/30e714f91194c86c89154820ad6990cc.pdf>
