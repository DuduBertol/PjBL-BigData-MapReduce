# PjBL-BigData-MapReduce

Você foi contratado para integrar a equipe de análise de dados de uma grande empresa
multinacional. Nessa equipe, é utilizada a tecnologia Hadoop para processar grandes bases de dados
utilizando a linguagem de programação Java. No projeto atual, você deverá utilizar o modelo de
programação MapReduce para extrair uma série de informações sobre transações comerciais
internacionais realizadas pela empresa nos últimos 30 anos. Essas transações estão armazenadas em um
dataset estruturado com 10 colunas, conforme a descrição apresentada na tabela abaixo:

De acordo com o contexto apresentado acima, você e sua equipe são responsáveis por
desenvolver soluções em MapReduce capazes de responder as seguintes perguntas:
1. (0,2 ponto) Número de transações envolvendo o Brasil.
2. (0,2 ponto) Número de transações por ano.
3. (0,2 ponto) Número de transações por categoria.
4. (0,2 ponto) Número de transações por tipo de fluxo (flow).
5. (0,4 ponto) Valor médio das transações por ano somente no Brasil. Necessário criar um
writable customizado.
6. (0,4 ponto) Transação mais cara e mais barata no Brasil em 2016. Uso obrigatório do
Combiner e criação de um writable customizado.
7. (0,4 ponto) Valor médio das transações por ano, considerando somente as transações do
tipo exportação (Export) realizadas no Brasil. Uso obrigatório do Combiner.
8. (0,5 ponto) Transação com o maior e menor preço (com base na coluna amount), por ano e
país. Obrigatório o uso de um Comparable writable e Combiner.

Para cada um dos itens acima, forneça:
1. Será necessário retirar o cabeçalho.
2. Será necessário tratar dados faltantes.
3. Código fonte para a resolução do problema utilizando MapReduce em Java. ATENÇÃO: não
serão consideradas como corretas, soluções que realizam a concatenação de strings para a
formação de chaves ou valores compostos.
4. O resultado da execução em um arquivo separado e no formato txt.
