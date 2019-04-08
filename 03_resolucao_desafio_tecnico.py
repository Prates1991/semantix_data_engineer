sc = SparkContext.getOrCreate()

rdd_log_nasa = sc.textFile("./dados/access_log_Jul95,./dados/access_log_Aug95")

################################################################################
## 1 - Número​ ​de​ ​hosts​ ​únicos | R- 137.978
################################################################################
# No primeiro "map" as linhas são separadas com base no delimitador " - - [" deixando assim, a primeira coluna apenas com os valores de host
# No segundo "map" são criados os conjuntos de chave e valor que serão utilizados no passo seguinte
# Na etapa do "reduceByKey" é onde ocorre a redução/agregação e a somatória de cada chave e seu respectivo valor
# Por fim, é somada a quantidade de chaves distintas existentes com os comandos ".heys().count()"
count_hosts = rdd_log_nasa.map(lambda cols: cols.split(" - - [")).map(lambda cols: (cols[0],1)).reduceByKey(lambda a,b: a + b).keys().count()

print(count_hosts)
# 137.978

################################################################################
## 2 - O​ ​total​ ​de​ ​erros​ ​404 | R - 20.901
################################################################################

# No primeiro "map" as linhas são separadas com base no delimitador " "
# No segundo "map" são criados os conjuntos de chave e valor utilizando a penúltima coluna que corresponde ao Código de Retorno HTTP
# Na etapa do "reduceByKey" é onde ocorre a redução/agregação e a somatória de cada chave seu respectivo valor
error_404 = rdd_log_nasa.map(lambda cols: cols.split(' ')).map(lambda cols: (cols[-2],1)).reduceByKey(lambda a,b: a + b)


error_404.collect()
# [('400', 15),
#  ('304', 266773),
#  ('200', 3100522),
#  ('302', 73070),
#  ('403', 225),
#  ('500', 65),
#  ('501', 41),
#  ('404', 20901)]

################################################################################
## 3 - Os​ ​5​ ​URLs​ ​que​ ​mais​ ​causaram​ ​erro​ ​404
################################################################################

# No primeiro "map" as linhas são separadas com base no delimitador " "
# O comando "filter" seleciona apenas as linhas cujo Código de Retorno HTTP é igual a 404
# No segundo "map" são criados os conjuntos de chave e valor utilizando o Código de Retorno HTTP como chave e o valor igual a 1
# Na etapa do "reduceByKey" é onde ocorre a redução/agregação e a somatória de cada chave e seu respectivo valor
# Por fim, os itens são ordenados com os valores em forma decrescente
url_404_top_5 = rdd_log_nasa.map(lambda cols: cols.split(' ')) \
                            .filter(lambda cols: "404" in cols[-2]) \
                            .map(lambda cols: (cols[-4],1)) \
                            .reduceByKey(lambda a,b: a + b) \
                            .takeOrdered(5, key = lambda r: -r[1])

url_404_top_5
# [('/pub/winvn/readme.txt', 2004),
# ('/pub/winvn/release.txt', 1732),
# ('/shuttle/missions/STS-69/mission-STS-69.html', 682),
# ('/shuttle/missions/sts-68/ksc-upclose.gif', 426),
# ('/history/apollo/a-001/a-001-patch-small.gif', 384)]

################################################################################
## 4 - Quantidade​ ​de​ ​erros​ ​404​ ​por​ ​dia | R - 360.34
################################################################################

# No primeiro "map" as linhas são separadas com base no delimitador "["
# No segundo "map" o item cols[1] de cada linha é separada com base no delimitador " "
# O comando "filter" seleciona apenas as linhas cujo Código de Retorno HTTP é igual a 404
# No terceiro "map" o item cols[0] de cada linha é separada com base no delimitador ":" deixando assim apenas a data completa em evidência "dd/mm/aaaa"
# No quarto "map" são criados os conjuntos de chave e valor utilizando a data completa "dd/mm/aaaa" como chave e o valor igual a 1
# Na etapa do "reduceByKey" é onde ocorre a redução/agregação e a somatória de cada chave e seu respectivo valor
error_404_a_day = rdd_log_nasa.map(lambda cols: cols.split('[')) \
                                .map(lambda cols: cols[1].split(' ')) \
                                .filter(lambda cols: "404" in cols[-2]) \
                                .map(lambda cols: cols[0].split(':')) \
                                .map(lambda cols: (cols[0],1)) \
                                .reduceByKey(lambda a,b: a + b) \

# Comando utilizado para contar a quantidade de chaves diverentes (dias)
error_404_total_days = error_404_a_day.keys().count()

# Comando utlizado para se obter o total de solicitações do período em análise
error_404_total_errors = error_404_a_day.values().reduce(lambda a,b:a+b)

# Média obtida através da dividão da quantidade total de solicitações com erro 404 pela quantidade de chaves diferentes (dias)
average_erros_404_a_day = error_404_total_errors / error_404_total_days

# Média de solicitações com erro 404 por dia
print(average_erros_404_a_day)
# 360.3448275862069

################################################################################
## 5 - O​ ​total​ ​de​ ​bytes​ ​retornados | R - 65.524.314.915
################################################################################

# No primeiro "map" as linhas são separadas com base no delimitador " "
# O comando "filter" seleciona apenas as linhas cujo total de byter retornados é diferente de "-"
# No segundo "map" são criados os conjuntos de chave e valor utilizando o Código de Retorno HTTP como chave e o valor igual ao total​ ​de​ ​bytes​ ​retornados
# No terceiro "map" os valores dos totais​ ​de​ ​bytes​ ​retornados são transformados em valores inteiros
# Por fim, o comando reduce é utlizado para se obter a somatória total de bytes retornados
total_bytes = rdd_log_nasa.map(lambda cols: cols.split(' ')) \
                            .filter(lambda cols: "-" not in cols[-1]) \
                            .map(lambda cols: (cols[-2], cols[-1])) \
                            .map(lambda cols: int(cols[1])) \
                            .reduce(lambda a,b: a + b)

print(total_bytes)
# 65.524.314.915                            