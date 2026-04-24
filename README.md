# Auditoria de Dados e Conciliação Financeira: Concessionária x Fabricante

## Contexto de Negócio
Em operações de grande porte, como a gestão de frotas e equipamentos pesados, a eficiência financeira depende da agilidade no fluxo de garantias. Um problema crítico neste setor é o "hiato de informação": processos que são aprovados operacionalmente, mas que, por falhas de integração ou erro humano, não são devidamente conciliados entre os controlos internos da concessionária e o portal do fabricante (CAT).

Este projeto simula a auditoria de um backlog de 3 anos, focando na identificação de capital retido — valores que a empresa tem direito a receber, mas que estão "escondidos" devido a inconsistências de dados.

## Objetivo
Desenvolver um pipeline de dados ponta a ponta para integrar, higienizar e auditar processos de garantia, permitindo a recuperação de receitas através da identificação de falhas de conciliação.

## Estrutura do Projeto
O pipeline está dividido em três camadas lógicas, simulando um ambiente real de Engenharia de Dados:

| Arquivo | Função Técnica | Habilidades Demonstradas |
| :--- | :--- | :--- |
| **`01_extracao_cat_sotreq.sql`** | Extração analítica cruzando a base interna com o Portal CAT. Realiza o cálculo de *aging* e define a prioridade de auditoria. | SQL Avançado, CTEs, Regras de Negócio. |
| **`backlog_garantias_raw.csv`** | Dataset bruto (200+ linhas) com inconsistências propositais: datas em múltiplos formatos, ruídos de texto e falhas de integração. | Data Discovery, Mapeamento de Inconsistências. |
| **`02_limpeza_data_quality.py`** | Motor de processamento em Python que valida o *schema*, limpa os dados e calcula o score de risco financeiro. | Python, Pandas (Vectorized), Data Quality. |

## Lógica de Auditoria e Inteligência Aplicada
O diferencial deste pipeline não é apenas limpar os dados, mas sim aplicar inteligência para suporte à decisão:
* **Cálculo de Risco:** Processos com *aging* superior a 365 dias recebem um incremento automático no risco de perda financeira.
* **Score de Prioridade:** O sistema cruza o valor pleiteado com a ausência de protocolo no fabricante para gerar um ranking de auditoria.
* **Validação de Schema:** O script Python interrompe a execução se as colunas vitais de conciliação não estiverem presentes, garantindo a integridade da Camada Prata.

## Decisões de Arquitetura
* **Separação de Responsabilidades:** Optei por processar a priorização inicial diretamente no SQL, reduzindo a carga de processamento na camada analítica (Python).
* **Performance:** Utilizei operações vetorizadas no Pandas (substituindo loops e `.apply()`) para garantir que o pipeline seja escalável para frotas maiores.
* **Resiliência:** Implementação de `logging` profissional para rastreabilidade de erros de integração entre as bases.

## Impacto e Resultados
Este modelo de auditoria permite transformar dados "sujos" e desconectados num relatório executivo de recuperação de receita. A aplicação de lógicas semelhantes em cenários reais permitiu a identificação e recuperação de **R$ 538.000,00** em reembolsos que estavam retidos por falhas de processo.

---
*Nota: Este projeto utiliza dados 100% simulados para demonstrar competências técnicas em SQL e Python aplicadas a desafios reais de conciliação financeira e auditoria de dados.*