Projeto de Auditoria de Garantias (Pipeline de Dados)
Contexto
Neste projeto, eu simulo um problema comum em operações que utilizam ERP (como SAP):
requisições de garantia que são aprovadas operacionalmente, mas não chegam a ser contabilizadas corretamente.
Esse tipo de falha acaba gerando um efeito silencioso, mas relevante: valores ficam “presos” sem visibilidade clara para o negócio.
A ideia aqui foi construir um pipeline simples, mas consistente, para identificar esse tipo de situação a partir de dados brutos.
________________________________________
Objetivo
Identificar, priorizar e quantificar valores potencialmente recuperáveis a partir de falhas de integração entre sistemas operacionais e o SAP.
________________________________________
Estrutura do projeto
Dividi o projeto em três partes, seguindo um fluxo comum de dados:
1. Extração e regras iniciais (SQL)
Arquivo: 01_extracao_sap_operacoes.sql
•	Cruzamento de dados operacionais com informações financeiras
•	Cálculo de aging (dias em aberto)
•	Criação de uma prioridade inicial de auditoria (prioridade_auditoria)
Aqui eu optei por já trazer parte da lógica de negócio na extração, simulando um cenário em que o dado não chega totalmente “cru” para a camada analítica.
________________________________________
2. Base bruta (CSV)
Arquivo: backlog_garantias_raw.csv
A base foi construída com inconsistências propositalmente inseridas, como:
•	Datas em formatos diferentes
•	Valores inválidos (zero, negativos e texto)
•	Problemas de integração (ERRO_SAP, valores nulos)
•	Variações de texto (maiúsculas, minúsculas, espaços)
A ideia foi simular o tipo de dado que normalmente vem de sistemas legados.
________________________________________
3. Processamento e qualidade de dados (Python)
Arquivo: 02_limpeza_data_quality.py
Nessa etapa eu faço o tratamento principal:
•	Padronização de strings e datas
•	Limpeza de valores não auditáveis
•	Identificação de falhas de integração com SAP
•	Cálculo de risco financeiro com base no aging
•	Criação de um score de priorização combinando valor e prioridade
Também incluí uma validação simples de schema e algumas métricas básicas de qualidade de dados para evitar que o pipeline quebre silenciosamente.
________________________________________
Lógica do pipeline
O fluxo segue uma estrutura direta:
Dados brutos → tratamento → enriquecimento → base auditável
No final, a saída é uma base “silver” com:
•	dados padronizados
•	registros válidos
•	classificação de risco
•	priorização para auditoria
________________________________________
Exemplo de regra de negócio
•	Registros com mais de 365 dias recebem aumento no risco financeiro
•	Casos sem documento contábil são tratados como falha de integração
•	A prioridade definida no SQL influencia diretamente o score final
________________________________________
Decisões técnicas
Algumas escolhas foram intencionais:
•	Mantive parte da lógica no SQL (priorização inicial) e parte no Python (cálculo de risco), para simular separação de responsabilidades
•	Evitei uso de .apply() no pandas, priorizando operações vetorizadas
•	Adicionei uma validação simples de colunas esperadas antes do processamento
________________________________________
Limitações
•	O cálculo de risco é simplificado e baseado apenas em aging
•	O projeto utiliza CSV em vez de banco de dados
•	Não há modelo preditivo ou estatístico
________________________________________
Possíveis evoluções
•	Integração com banco de dados
•	Criação de dashboard para acompanhamento
•	Modelos de previsão de perda financeira
•	Monitoramento contínuo de qualidade de dados
________________________________________
Resultado
Com esse pipeline, é possível identificar rapidamente:
•	quanto valor pode estar retido
•	quais casos devem ser priorizados
•	onde estão as falhas de integração
________________________________________
Aprendizados
Esse projeto me ajudou principalmente a:
•	estruturar um pipeline de dados ponta a ponta
•	separar responsabilidades entre SQL e Python
•	lidar com dados inconsistentes (cenário real)
•	transformar um problema de negócio em regras analíticas
________________________________________
Observação
Os dados utilizados são simulados, mas foram construídos para refletir problemas comuns em ambientes corporativos.