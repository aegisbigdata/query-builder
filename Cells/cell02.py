#P1

from pyspark.sql.functions import col, isnan, isnull, quarter, ntile, month, hour, minute, dayofmonth, dayofyear, to_date, unix_timestamp, trunc, datediff, months_between, sqrt, when
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F
from pyspark.sql.functions import unix_timestamp
from  pyspark.sql.functions import abs as cabs
from pyspark.sql.functions import round as pyround
import json
import requests
from hops import hdfs
import pydoop.hdfs as phdfs
from  pyspark.sql.functions import year, ltrim, rtrim
from pyspark.sql.functions import coalesce, lit

myhdfs = phdfs.hdfs(user = hdfs.project_user())

tempDF = None
masterDF = None
hdfspath = hdfs.project_path()
g=None
query_init="from pyspark.sql.functions import col, isnan, isnull, quarter, ntile, month, hour, minute, dayofmonth, dayofyear, to_date, unix_timestamp, trunc, datediff, months_between, sqrt, when, unix_timestamp, year, ltrim, rtrim, coalesce, lit \nfrom pyspark.sql.window import Window \nfrom pyspark.sql import Row, functions as F\nfrom  pyspark.sql.functions import abs as cabs \nfrom pyspark.sql.functions import round as pyround\n"
query = query_init

def apply_filters(applied_filters):
    global tempDF
    global masterDF
    applied_filters = json.loads(applied_filters)
    global g
    global query
    g = applied_filters
    filters_success = []
    for i in range(1,len(applied_filters)+1):
        index = str(i)
        filter_type = applied_filters[index]["type"]
        if applied_filters[index]["executed"]:
            continue  
        try:
            if filter_type=="filter":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                if applied_filters[index]["parameters"][1]=="like":
                    tempDF = tempDF.filter(applied_filters[index]["parameters"][0]+" "+applied_filters[index]["parameters"][1]+" \'"+applied_filters[index]["parameters"][2][:-1]+"%\'")
                    query+="tempDF = tempDF.filter(\""+applied_filters[index]["parameters"][0]+" "+applied_filters[index]["parameters"][1]+" \'"+applied_filters[index]["parameters"][2][:-1]+"%\'\")\n"
                else:
                    try:
                        float(applied_filters[index]["parameters"][2])
                        tempDF = tempDF.filter(" ".join(applied_filters[index]["parameters"]))
                        query+="tempDF = tempDF.filter(\""+" ".join(applied_filters[index]["parameters"])+"\")\n"
                    except ValueError as ver:
                        applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                        tempDF = tempDF.filter(" ".join(applied_filters[index]["parameters"]))
                        query+="tempDF = tempDF.filter(\""+" ".join(applied_filters[index]["parameters"])+"\")\n"
            if filter_type=="filterOr":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                if applied_filters[index]["parameters"][1]=="like":
                    cond1 = applied_filters[index]["parameters"][0]+" "+applied_filters[index]["parameters"][1]+" \'"+applied_filters[index]["parameters"][2][:-1]+"%\'"
                else:
                    try:
                        float(applied_filters[index]["parameters"][2])
                    except ValueError as ver:       
                        applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                    cond1 = " ".join(applied_filters[index]["parameters"][:3])

                applied_filters[index]["parameters"][3] = "`"+applied_filters[index]["parameters"][3]+"`"
                if applied_filters[index]["parameters"][4]=="like":
                    cond2 = applied_filters[index]["parameters"][3]+" "+applied_filters[index]["parameters"][4]+" \'"+applied_filters[index]["parameters"][5][:-1]+"%\'"
                else:
                    try:
                        float(applied_filters[index]["parameters"][5])
                    except ValueError as ver:       
                        applied_filters[index]["parameters"][5] = "'"+applied_filters[index]["parameters"][5]+"'"
                    cond2 = " ".join(applied_filters[index]["parameters"][3:6])

                tempDF = tempDF.filter(cond1 + " or " + cond2)
                query+="tempDF = tempDF.filter(\""+cond1 + " or " + cond2+"\")\n"
            elif filter_type=="join":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                keep = [x for x in paramsList[1:] if (None not in x)]
                jhow = str(paramsList[0])
                if len(keep) == 1:
                    tempDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1])],how=jhow)
                elif len(keep) == 2:
                    tempDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1]),col('masterDF.'+keep[1][0])==col('tempDF.'+keep[1][1])],how=jhow)
                elif len(keep) == 3:
                    tempDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1]),col('masterDF.'+keep[1][0])==col('tempDF.'+keep[1][1]),col('masterDF.'+keep[2][0])==col('tempDF.'+keep[2][1])],how=jhow)
            elif filter_type=="year":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],year(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",year(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="quarter":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],quarter(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",quarter(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="month":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],month(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",month(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="hour":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],hour(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",hour(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="minute":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],minute(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",minute(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="dayofmonth":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],dayofmonth(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",dayofmonth(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="dayofyear":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],dayofyear(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",dayofyear(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="to_date":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],to_date(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",to_date(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="unix_timestamp":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],unix_timestamp(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",to_timestamp(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="ytrunc":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],trunc(tempDF[applied_filters[index]["parameters"][0]],"year"))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",trunc(tempDF[\""+applied_filters[index]["parameters"][0]+"\"],\"year\"))\n"
            elif filter_type=="mtrunc":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],trunc(tempDF[applied_filters[index]["parameters"][0]],"month"))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",trunc(tempDF[\""+applied_filters[index]["parameters"][0]+"\"],\"month\"))\n"
            elif filter_type=="sqrt":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = applied_filters[index]["parameters"][1].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][1],sqrt(tempDF[applied_filters[index]["parameters"][0]]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][1]+"\",sqrt(tempDF[\""+applied_filters[index]["parameters"][0]+"\"]))\n"
            elif filter_type=="round":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = int(applied_filters[index]["parameters"][1])
                applied_filters[index]["parameters"][2] = applied_filters[index]["parameters"][2].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][2],pyround(tempDF[applied_filters[index]["parameters"][0]],applied_filters[index]["parameters"][1]))
                query += "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][2]+"\",pyround(tempDF[\""+applied_filters[index]["parameters"][0]+"\"],"+str(applied_filters[index]["parameters"][1])+"))\n"
            elif filter_type=="when_1":
                then_value = None
                else_value = None
                new_col_name = applied_filters[index]["parameters"][5].replace(" ","_")
                applied_filters[index]["parameters"][0] = "tempDF['`"+applied_filters[index]["parameters"][0]+"`']"
                applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                condition = " ".join(applied_filters[index]["parameters"][0:3])
                evaluated_condition = eval(condition)
                try:
                    then_value = float(applied_filters[index]["parameters"][3])
                except ValueError as ver:
                    then_value = applied_filters[index]["parameters"][3]
                try:
                    else_value = float(applied_filters[index]["parameters"][4])
                except ValueError as ver:
                    else_value = applied_filters[index]["parameters"][4]
                tempDF = tempDF.withColumn(new_col_name,when(evaluated_condition,then_value).otherwise(else_value))
                query+="tempDF = tempDF.withColumn(\""+new_col_name+"\",when("+condition+",\""+str(then_value)+"\").otherwise(\""+str(else_value)+"\"))\n"
            elif filter_type=="when_2":
                new_col_name = applied_filters[index]["parameters"][9].replace(" ","_")
                applied_filters[index]["parameters"][0] = "tempDF['`"+applied_filters[index]["parameters"][0]+"`']"
                applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                condition = " ".join(applied_filters[index]["parameters"][0:3])
                evaluated_condition = eval(condition)
                applied_filters[index]["parameters"][3] = "tempDF['`"+applied_filters[index]["parameters"][3]+"`']"
                applied_filters[index]["parameters"][5] = "'"+applied_filters[index]["parameters"][5]+"'"
                then_condition = " ".join(applied_filters[index]["parameters"][3:6])
                evaluated_then_condition = eval(then_condition)
                applied_filters[index]["parameters"][6] = "tempDF['`"+applied_filters[index]["parameters"][6]+"`']"
                applied_filters[index]["parameters"][8] = "'"+applied_filters[index]["parameters"][8]+"'"
                else_condition = " ".join(applied_filters[index]["parameters"][6:9])
                evaluated_else_condition = eval(else_condition)
                tempDF = tempDF.withColumn(new_col_name,when(evaluated_condition,evaluated_then_condition).otherwise(evaluated_else_condition))
                query+= "tempDF = tempDF.withColumn(\""+new_col_name+"\",when("+condition+","+then_condition+").otherwise("+else_condition+"))\n"
            elif filter_type=="when_3":
                new_col_name = applied_filters[index]["parameters"][9].replace(" ","_")
                applied_filters[index]["parameters"][0] = "tempDF['`"+applied_filters[index]["parameters"][0]+"`']"
                applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                condition = " ".join(applied_filters[index]["parameters"][0:3])
                evaluated_condition = eval(condition)
                applied_filters[index]["parameters"][3] = "tempDF['`"+applied_filters[index]["parameters"][3]+"`']"
                applied_filters[index]["parameters"][5] = "tempDF['`"+applied_filters[index]["parameters"][5]+"`']"
                then_condition = " ".join(applied_filters[index]["parameters"][3:6])
                evaluated_then_condition = eval(then_condition)
                applied_filters[index]["parameters"][6] = "tempDF['`"+applied_filters[index]["parameters"][6]+"`']"
                applied_filters[index]["parameters"][8] = "tempDF['`"+applied_filters[index]["parameters"][8]+"`']"
                else_condition = " ".join(applied_filters[index]["parameters"][6:9])
                evaluated_else_condition = eval(else_condition)
                tempDF = tempDF.withColumn(new_col_name,when(evaluated_condition,evaluated_then_condition).otherwise(evaluated_else_condition))
                query+= "tempDF = tempDF.withColumn(\""+new_col_name+"\",when("+condition+","+then_condition+").otherwise("+else_condition+"))\n"
            elif filter_type=="col_operation_1":
                new_col_name = applied_filters[index]["parameters"][3].replace(" ","_")
                applied_filters[index]["parameters"][0] = "tempDF['`"+applied_filters[index]["parameters"][0]+"`']"
                applied_filters[index]["parameters"][2] = "'"+applied_filters[index]["parameters"][2]+"'"
                expr = " ".join(applied_filters[index]["parameters"][0:3])
                evaluated_expr = eval(expr)
                tempDF = tempDF.withColumn(new_col_name,evaluated_expr)
                query+="tempDF = tempDF.withColumn(\""+new_col_name+"\","+expr+")\n"
            elif filter_type=="col_operation_2":
                new_col_name = applied_filters[index]["parameters"][3].replace(" ","_")
                applied_filters[index]["parameters"][0] = "tempDF['`"+applied_filters[index]["parameters"][0]+"`']"
                applied_filters[index]["parameters"][2] = "tempDF['`"+applied_filters[index]["parameters"][2]+"`']"
                expr = " ".join(applied_filters[index]["parameters"][0:3])
                evaluated_expr = eval(expr)
                tempDF = tempDF.withColumn(new_col_name,evaluated_expr)
                query+="tempDF = tempDF.withColumn(\""+new_col_name+"\","+expr+")\n"
            elif filter_type=="datediff":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = "`"+applied_filters[index]["parameters"][1]+"`"
                applied_filters[index]["parameters"][2] = applied_filters[index]["parameters"][2].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][2],datediff(tempDF[applied_filters[index]["parameters"][0]],tempDF[applied_filters[index]["parameters"][1]]))
                query+="tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][2]+"\",datediff(tempDF[\""+applied_filters[index]["parameters"][0]+"\"],tempDF[\""+applied_filters[index]["parameters"][1]+"\"]))\n"
            elif filter_type=="months_between":
                applied_filters[index]["parameters"][0] = "`"+applied_filters[index]["parameters"][0]+"`"
                applied_filters[index]["parameters"][1] = "`"+applied_filters[index]["parameters"][1]+"`"
                applied_filters[index]["parameters"][2] = applied_filters[index]["parameters"][2].replace(" ","_")
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][2],months_between(tempDF[applied_filters[index]["parameters"][0]],tempDF[applied_filters[index]["parameters"][1]]))
                query+="tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][2]+"\",months_between(tempDF[\""+applied_filters[index]["parameters"][0]+"\"],tempDF[\""+applied_filters[index]["parameters"][1]+"\"]))\n"
            elif filter_type=="orderBy":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                l = ["`"+x+"`" for x in list(paramsList[0])]
                tempDF = tempDF.orderBy(l,ascending = list(paramsList[1]))
                query+="tempDF = tempDF.orderBy(\""+l[0]+"\",ascending = "+str(list(paramsList[1]))+")\n"
            elif filter_type=="drop":
                tempDF = tempDF.drop(*applied_filters[index]["parameters"][0])
                query+="tempDF = tempDF.drop(*"+str(applied_filters[index]["parameters"][0])+")\n"
            elif filter_type=="withColumnRenamed":
                tempDF = tempDF.withColumnRenamed(applied_filters[index]["parameters"][0],applied_filters[index]["parameters"][1])
                query+="tempDF = tempDF.withColumnRenamed(\""+applied_filters[index]["parameters"][0]+"\",\""+applied_filters[index]["parameters"][1]+"\")\n"
            elif filter_type=="select":
                l = ["`"+x+"`" for x in applied_filters[index]["parameters"][0]]
                tempDF = tempDF.select(*l)
                query+="tempDF = tempDF.select(*"+str(l)+")\n"
            elif filter_type=="dropDuplicates":
                tempDF = tempDF.dropDuplicates()
                query+="tempDF = tempDF.dropDuplicates()\n"
            elif filter_type=="fillna":
                fillDict = dict(applied_filters[index]["parameters"][0])
                tempDF = tempDF.fillna(fillDict)
                query+="tempDF = tempDF.fillna("+str(fillDict)+")\n"
            elif filter_type=="replace":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                tempDF = tempDF.replace(paramsList[0],paramsList[1],str(paramsList[2]))
                query+="tempDF = tempDF.replace(list("+str(paramsList[0])+"),list("+str(paramsList[1])+"),(\""+str(paramsList[2])+"\"))\n"
            elif filter_type=="min":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                colsp = list(paramsList[1])
                cols = ["`"+x+"`" for x in colsp]
                gby = ["`"+y+"`" for y in list(paramsList[0])]
                tempDF = tempDF.groupBy(gby).min(*cols)
                query+="tempDF = tempDF.groupBy(\""+gby[0]+"\").min(*"+str(cols)+")\n"
            elif filter_type=="max":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                colsp = list(paramsList[1])
                cols = ["`"+x+"`" for x in colsp]
                gby = ["`"+y+"`" for y in list(paramsList[0])]
                tempDF = tempDF.groupBy(gby).max(*cols)
                query+="tempDF = tempDF.groupBy(\""+gby[0]+"\").max(*"+str(cols)+")\n"
            elif filter_type=="avg":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                colsp = list(paramsList[1])
                cols = ["`"+x+"`" for x in colsp]
                gby = ["`"+y+"`" for y in list(paramsList[0])]
                tempDF = tempDF.groupBy(gby).avg(*cols)
                query+="tempDF = tempDF.groupBy(\""+gby[0]+"\").avg(*"+str(cols)+")\n"
            elif filter_type=="sum":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                colsp = list(paramsList[1])
                cols = ["`"+x+"`" for x in colsp]
                gby = ["`"+y+"`" for y in list(paramsList[0])]
                tempDF = tempDF.groupBy(gby).sum(*cols)
                query+="tempDF = tempDF.groupBy(\""+gby[0]+"\").sum(*"+str(cols)+")\n"
            elif filter_type=="count":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                l = ["`"+x+"`" for x in paramsList[0]]
                tempDF = tempDF.groupBy(list(l)).count()
                query+="tempDF.groupBy(\""+list(l)[0]+"\").count()\n"
            elif filter_type=="isnan":
                cols = "`"+applied_filters[index]["parameters"][0]+"`"
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][0]+"_isnan",isnan(tempDF[cols]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][0]+"_isnan\",isnan(tempDF[\""+cols+"\"]))\n"
            elif filter_type=="isnull":
                cols = "`"+applied_filters[index]["parameters"][0]+"`"
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][0]+"_isnull",isnull(tempDF[cols]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][0]+"_isnull\",isnull(tempDF[\""+cols+"\"]))\n"
            elif filter_type=="ltrim":
                cols = "`"+applied_filters[index]["parameters"][0]+"`"
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][0]+"_ltrimmed",ltrim(tempDF[cols]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][0]+"_ltrimmed\",ltrim(tempDF[\""+cols+"\"]))\n"
            elif filter_type=="rtrim":
                cols = "`"+applied_filters[index]["parameters"][0]+"`"
                tempDF = tempDF.withColumn(applied_filters[index]["parameters"][0]+"_rtrimmed",rtrim(tempDF[cols]))
                query+= "tempDF = tempDF.withColumn(\""+applied_filters[index]["parameters"][0]+"_rtrimmed\",rtrim(tempDF[\""+cols+"\"]))\n"
            elif filter_type=="joinApproxTime":
                paramsList = [x for x in applied_filters[index]["parameters"]]
                keep = [x for x in paramsList[1:-1] if (None not in x)]
                jhow = str(paramsList[0])
                time_columns = paramsList[-1]
                allcols = tempDF.columns
                allcols.extend(masterDF.columns)
                if len(keep) == 1:
                    joinedDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1])],how=jhow)
                    additional_col = joinedDF.withColumn("time_diff",  cabs(unix_timestamp(str(time_columns[0])) - unix_timestamp(str(time_columns[1]))))
                    partDF = additional_col.select(F.row_number().over(Window.partitionBy(str(keep[0][0])).orderBy("time_diff")).alias("rank"), "time_diff",*allcols ).filter(col("rank") == 1)
                    tempDF = partDF
                elif len(keep) == 2:
                    joinedDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1]),col('masterDF.'+keep[1][0])==col('tempDF.'+keep[1][1])],how=jhow)
                    additional_col = joinedDF.withColumn("time_diff",  cabs(unix_timestamp(str(time_columns[0])) - unix_timestamp(str(time_columns[1]))))
                    partDf = additional_col.select( F.row_number().over(Window.partitionBy(str(keep[0][0]),str(keep[1][0])).orderBy("time_diff")).alias("rank"), *allcols ).filter(col("rank") == 1).drop('rank')
                    tempDF = partDF
                elif len(keep) == 3:
                    joinedDF = masterDF.alias('masterDF').join(tempDF.alias('tempDF'),on=[col('masterDF.'+keep[0][0])==col('tempDF.'+keep[0][1]),col('masterDF.'+keep[1][0])==col('tempDF.'+keep[1][1]),col('masterDF.'+keep[2][0])==col('tempDF.'+keep[2][1])],how=jhow)
                    additional_col = joinedDF.withColumn("time_diff",  cabs(unix_timestamp(str(time_columns[0])) - unix_timestamp(str(time_columns[1]))))
                    partDf = additional_col.select( F.row_number().over(Window.partitionBy(str(keep[0][0]),str(keep[1][0]),str(keep[2][0])).orderBy("time_diff")).alias("rank"), *allcols).filter(col("rank") == 1).drop('rank')
                    tempDF = partDF
            filters_success.append(index)
        except Exception as e:
            g = e
            return
    return filters_success

def read_dataset(selectedFile, separator, encoding):
    global tempDF
    global query
    global g
    if selectedFile.startswith('/'):
        selectedFile = "hdfs://" + myhdfs.host + ":" + str(myhdfs.port) + selectedFile
    try:
        if encoding:
            tempDF = spark.read.csv(selectedFile, header=True, inferSchema=True, sep=separator, encoding=encoding)
        else:
            tempDF = spark.read.csv(selectedFile, header=True, inferSchema=True, sep=separator)
    except Exception as e:
        g=e
    query=query_init
    return tempDF.columns

def getTempColumns():
    global tempDF
    return tempDF.columns

def tempDFtoMasterDF():
    global masterDF
    global tempDF
    masterDF = tempDF
    return masterDF.columns

def saveMasterAsCSV(savepath, withheader):
    global masterDF
    global hdfspath
    global g
    hdfs_start_path = "hdfs://"+myhdfs.host+":"+str(myhdfs.port)
    completepath = hdfs_start_path + savepath
    if withheader == 1:
        headerflag = True
    else:
        headerflag = False
    try:
        masterDF.coalesce(1).write.csv(path=completepath, mode='append', header=headerflag)
        return 200
    except Exception as e:
        g=e

def getCSVfiles():
    fileSet = set()
    for f in myhdfs.walk(hdfs.project_path()):
        fileSet.add(f['name'])
    available_files = [x for x in list(fileSet) if x.endswith(".csv")]
    return available_files
    
def find_current_projectID(pDict):
    projectsDict = json.loads(pDict)
    pname = hdfs.project_name()
    return projectsDict[pname] #KeyError should not be possible here, so no try-except...
    
def walk_dataset(dataset_path):
    fileSet = set()
    hdfs_start_path = "hdfs://"+myhdfs.host+":"+str(myhdfs.port)
    for f in myhdfs.walk(hdfs_start_path + dataset_path):
        fileSet.add(f['name'])
    available_files = [x for x in list(fileSet) if x.endswith(".csv")]
    return available_files