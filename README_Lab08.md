# Lab 08 — Filas e pipelines: threads a passar trabalho umas às outras

**Universidade Politécnica da Guarda** · Licenciatura em Ciência de Dados e Inteligência Artificial · Programação Paralela e Concorrente · 2026/2027

## Objetivos

- Construir um pipeline ETL com três etapas ligadas por `queue.Queue`.
- Utilizar sentinelas para terminar as etapas e filas limitadas para aplicar *backpressure*.
- Comparar a repartição dinâmica do trabalho com a utilização de prioridades.
- Analisar a segurança de estruturas de dados partilhadas entre threads.

## Parte A — Pipeline ETL

O pipeline processa 40 ficheiros CSV com dados simulados de sensores. O processamento está dividido em três etapas: leitura dos ficheiros, transformação dos dados e escrita dos resultados em `resumo.csv`. As etapas comunicam através de filas.

### A1 — Verificação dos resultados

Na execução com `uv run python pipeline_etl.py --sequencial`, foram apresentados os seguintes resultados:

- **Ficheiros processados:** 40
- **Leituras válidas:** 158 385
- **Soma das médias por ficheiro:** 915,3542
- **Resultado da versão concorrente:** igual ao da versão sequencial
- **Speedup observado nessa execução:** 3,82×

A execução do verificador apresentou posteriormente um speedup de 4,05×. Esta diferença é normal, uma vez que os tempos de execução podem variar entre execuções. O aspeto essencial é que os resultados da versão concorrente coincidiram com os da versão sequencial.

A soma das médias corresponde à soma das médias calculadas individualmente para cada ficheiro, não à média global de todas as leituras.

### A2 — Etapas e sentinelas

O pipeline contém três etapas:

1. **Leitoras:** leem os ficheiros CSV e colocam o seu conteúdo na primeira fila.
2. **Transformadoras:** convertem os valores, excluem leituras inválidas e calculam estatísticas por ficheiro.
3. **Escritora:** recebe os resultados e escreve-os em `resumo.csv`.

A **sentinela** é um valor especial (`None`) colocado na fila para indicar a uma etapa que já não existem mais elementos para processar. É necessário enviar uma sentinela por cada thread que esteja à espera de trabalho. As sentinelas da primeira fila só são colocadas depois de as leitoras terminarem, garantindo que os ficheiros já foram enviados para transformação.

### A3 — Medição do débito

Foram testadas três configurações de threads:

| Leitoras | Transformadoras | Tempo (s) | Débito (ficheiros/s) |
|---:|---:|---:|---:|
| 1 | 1 | 0,87 | 45,9 |
| 2 | 4 | 0,22 | 180,4 |
| 2 | 8 | 0,12 | 339,9 |

O aumento do número de transformadoras reduziu o tempo de execução e aumentou o débito. Comparando a configuração de uma leitora e uma transformadora com a configuração de duas leitoras e oito transformadoras, o tempo desceu de 0,87 s para 0,12 s. O speedup aproximado, calculado a partir destes tempos arredondados, é de 7,25×.

Em todas as configurações, a verificação apresentou **158 385 leituras válidas** e **915,3542** como soma das médias. Assim, o aumento do paralelismo melhorou o desempenho sem alterar os resultados numéricos apresentados.

A transformação inclui uma espera artificial de 20 ms por ficheiro, pelo que pode constituir um gargalo do pipeline. Os ganhos medidos dependem da carga de trabalho, da configuração e dos custos de coordenação entre threads.

### A4 — Backpressure

Com o comando `uv run python pipeline_etl.py --maxsize 2`, foram obtidos os seguintes resultados:

- **Tempo:** 0,22 s
- **Débito:** 179,5 ficheiros/s
- **Máximo observado na primeira fila:** 2 elementos
- **Leituras válidas:** 158 385
- **Soma das médias:** 915,3542

O **backpressure** é um mecanismo de controlo do fluxo que impede uma etapa produtora de continuar a colocar elementos numa fila quando esta está cheia. Com `maxsize=2`, uma leitora que tente inserir um elemento numa fila cheia fica bloqueada até existir espaço disponível. Isto limita a acumulação de trabalho em memória e permite que o ritmo de produção se adapte à capacidade de processamento da etapa seguinte.

Neste teste, o débito foi praticamente igual ao da configuração com duas leitoras e quatro transformadoras sem limite explícito da fila (180,4 ficheiros/s). A pequena diferença pode resultar da variação normal dos tempos de execução. Em sistemas reais, filas sem limite podem acumular grandes quantidades de pedidos quando a produção é mais rápida do que o processamento, aumentando o consumo de memória e a latência.

## Parte B — Produtor-consumidor e prioridades

O `scraper.py` simula o processamento de 60 URLs com durações diferentes. Os consumidores retiram dinamicamente o próximo elemento da fila quando ficam disponíveis.

### B1 — Número de consumidores

| Consumidores | Tempo (s) | Speedup | Speedup ideal |
|---:|---:|---:|---:|
| 1 | 16,21 | 1,00× | 1× |
| 2 | 8,14 | 1,99× | 2× |
| 4 | 4,19 | 3,87× | 4× |
| 8 | 2,22 | 7,27× | 8× |
| 16 | 1,27 | 12,75× | 16× |

O aumento do número de consumidores reduziu significativamente o tempo total. Com 16 consumidores, o tempo passou de 16,21 s para 1,27 s, correspondendo a um speedup de 12,75×. O resultado ficou abaixo do ideal de 16×, o que mostra que a escalabilidade não é perfeitamente linear. Entre os fatores que influenciam o resultado estão a coordenação das threads, a produção dos URLs e os custos de gestão da concorrência.

A verificação apresentou **167 000 bytes** em todas as execuções.

### B2 — Fila normal e `PriorityQueue`

Na execução com quatro consumidores e uma fila normal (`Queue`), os URLs urgentes — identificados pelos índices múltiplos de 10 — terminaram nas posições **1, 14, 22, 33, 42 e 51**.

Com `uv run python scraper.py --prioridade`, os seis URLs urgentes terminaram nas posições **1, 5, 6, 7, 8 e 10**, respetivamente. O tempo total foi de 4,18 s, muito próximo dos 4,19 s obtidos com a fila normal.

A `PriorityQueue` dá prioridade aos elementos urgentes que ainda estão à espera na fila. Contudo, não interrompe o trabalho que um consumidor já começou a executar; por isso, a prioridade influencia a ordem de conclusão, mas não garante que todas as tarefas urgentes terminem imediatamente. Neste teste, a principal vantagem foi antecipar a conclusão dos URLs urgentes, sem alterar significativamente o tempo total.

No modelo de repartição fixa do Lab 05, cada thread receberia um conjunto predeterminado de tarefas. Se uma thread recebesse tarefas demoradas, poderia continuar ocupada enquanto outras threads já estivessem livres, reduzindo o aproveitamento dos recursos. Na repartição dinâmica, os consumidores livres podem retirar novas tarefas da fila, equilibrando melhor o trabalho.

## Parte C — Segurança das estruturas partilhadas

O programa `thread_safe.py` utiliza oito threads, cada uma com 20 000 operações, perfazendo **160 000 operações esperadas**.

| Estrutura/operação | Resultado observado | Conclusão |
|---|---:|---|
| `lista.append` | 160 000 de 160 000 em cada execução | OK nos testes realizados |
| `dict get + set` | 115 271; 117 818; 114 840 | Foram perdidas atualizações |
| `queue.Queue.put` | 160 000 de 160 000 em cada execução | OK nos testes realizados |

A atualização `d[chave] = d.get(chave, 0) + 1` envolve uma sequência de leitura, incremento e escrita. Se várias threads executarem estes passos de forma intercalada, podem ler o mesmo valor antes de qualquer uma o atualizar e, depois, sobrescrever o resultado umas das outras. Esta condição de corrida explica a perda de atualizações no dicionário e a variação dos resultados entre execuções.

No teste realizado, `lista.append` e `queue.Queue.put` atingiram o total esperado. A `Queue` fornece mecanismos internos de sincronização para operações concorrentes. Ainda assim, a segurança deve ser avaliada de acordo com a operação, a implementação e a versão do Python: não se deve assumir que uma sequência composta de várias operações é automaticamente atómica só por existir o GIL.

## Parte D — Verificação final

Foi executado o comando:

```bash
uv run python verificar_lab08.py
```

O verificador terminou com o resultado:

```text
Resultado: 7 de 7 OK, 0 aviso(s), 0 falha(s)
```

As sete verificações passaram:

- Ficheiros do laboratório presentes.
- Pipeline ETL com resultado igual ao sequencial.
- `resumo.csv` gerado.
- Scraper com todos os URLs processados nas execuções com 1 e 8 consumidores.
- Seis URLs urgentes concluídos entre os primeiros dez no teste de prioridade.
- Comportamento esperado das estruturas partilhadas.
- README com tabela, sentinela, backpressure e conclusões.

O resultado confirma que o laboratório passou nos critérios automáticos definidos pelo verificador. Não substitui a confirmação de eventuais requisitos adicionais indicados pelo docente.

## Conclusões

1. O pipeline ETL demonstrou que a divisão do processamento por etapas concorrentes e a utilização de filas podem reduzir o tempo de execução, mantendo os resultados de verificação iguais aos da versão sequencial.
2. A limitação das filas permite aplicar backpressure e controlar a acumulação de elementos, enquanto a `PriorityQueue` permite favorecer tarefas urgentes que ainda estão à espera de processamento.
3. Os testes de segurança demonstraram que é essencial compreender a atomicidade das operações e utilizar mecanismos de sincronização adequados, uma vez que operações compostas sobre dados partilhados podem provocar condições de corrida e perda de atualizações.

## Nota para a entrega

Antes de fazer commit, confirmar que `lab08/dados/` e `lab08/resumo.csv` estão excluídos do Git através do `.gitignore`, conforme indicado no guião. Depois, verificar as alterações e fazer commit/push para o repositório do projeto, se aplicável.
