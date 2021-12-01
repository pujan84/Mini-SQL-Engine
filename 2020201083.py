import sqlparse
import csv
import sys
import re
import itertools 
import os
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML, Whitespace

meta_data={}
where_con=[]
col_order_by=[]
col_group_by=[]
col_projection=[]
aggr_list=[]
table=[]
order_desc=False
flag_select=flag_from=flag_where=flag_group_by=flag_order_by=flag_distinct=flag_is_star=False
col_table_mapping={}

def get_metadata():
    f = open ('metadata.txt','r')
    tmp=[] 
    flag = False 

    for row in f:      
        if(row.strip() == "<begin_table>"):
            flag = True   
        elif(flag == True):
            name = row.strip().lower()
            flag = False 
        elif(row.strip() == "<end_table>"):
            meta_data[name]=tmp
            tmp=[]
        else:
            col_table_mapping[row.strip().lower()]=name
            tmp.append(row.strip().lower())
    # print(col_table_mapping)
    # print(meta_data)

def find_cross_product(x, y):
    z = []
    for i in range(len(y)):
        for j in range(len(x)):
            tmp = []
            tmp.extend(x[j])
            tmp.extend(y[i])
            z.append(tmp)
    return z

def get_table(table_name):
    tmp_table = []
    ans_table = []
    for table in table_name:
        tmp_table = []
        f = table + '.csv'
        if os.path.exists(f):
            with open(f, 'r') as file:
                file_reader = csv.reader(file)
                for i in file_reader:
                    tmp_table.append(i)
            x=len(ans_table)
            if x != 0:
                ans_table = find_cross_product(ans_table, tmp_table)
            else:
                ans_table = tmp_table
        else:
            print(table + " is not exists.")
            exit()
    
    return ans_table

# def is_select(input):
#     if not input.is_group():
#         return false
#     for item in input.tokens:
#         if item.type is DML and item.value.upper()=='SELECT':
#             return true
#     return false

def get_col_index(table, col_name):
    col_index = []
    len_col=len(col_name)
    len_table=len(table)
    # print("IN GET COL INDEX",col_name)
    for k in range(len_col):
        x = 0
        for i in range(len_table):
                f = False
                for j in range(len(meta_data[table[i]])):		
                    # print("metadata",meta_data[table[i]][j])
                    # print("col_name",col_name[k].lower())	
                    if meta_data[table[i]][j] == col_name[k].lower():
                        col_index.append(x + j)
                        f = True
                        break
                
                if f == True:
                    break
                x = x + len(meta_data[table[i]])
        if len(col_index) == k:
            col_index.append(col_name[k])
    # print("In get_col_index",col_index)
    return col_index

def print_output(table_data,col_name,col_index):
    # print("table data",table_data)
    # print("col_name",col_name)
    # print("col_index",col_index)
    # flag_x=False
    row=len(table_data)
    

    for i in col_name:
    #     for k in meta_data:
    #         value=meta_data[k]
    #         for v in value:
    #             if v==i:
    #                 flag_x=True
    #                 break
    #     if flag_x==False:

    #         print("There is no column present in database named",i)
    #         exit()
        if i.lower() not in col_table_mapping:
            print()
            print("There is no column present in database named",i)
            exit()
        print(col_table_mapping[i.lower()]+"."+i,end="\t")
        # print("\t")
    print()
    for i in range(row):
        for j in col_index:
            print(table_data[i][j],end="\t")
            # print("\t")
        print()
        


def get_table_data_byindex(table_data,col):
    # print("In get table by index")
    global table
    row=len(table_data)
    if len(col) == 0:
        tmp_project=[]
        for i in table:
            y=meta_data[i]
            for j in y:
                tmp_project.append(j)
        # print("tmp_project",tmp_project)
        tmp_project_index=get_col_index(table,tmp_project)
        print_output(table_data,tmp_project,tmp_project_index)

    else:   
        # print("Col projection",col_projection)
        print_output(table_data,col_projection,col)
       

def check_operator(c):
	operator_list = [">", "<", "=", ">=", "<="]
	for i in operator_list:
		if i == c:
			return True
	return False

def findmax(table,agg_list,index):
    # print("Pujan Index",index)
    len1=len(table)
    x=sorted(table,key=lambda x: int(x[index]))
    # print(x)
    # print("pujan ans",x[0][index])
    return x[len1-1][index]


def findmin(table,agg_list,index):
    x=sorted(table,key=lambda x: int(x[index]))
    return x[0][index]

def findsum(table,agg_list,index):
    sum=0
    for i in range(len(table)):
        sum=sum+int(table[i][index])

        # print(sum)
    return sum

def findavg(table,agg_list,index):
    sum=0
    for i in range(len(table)):
        sum=sum+int(table[i][index])
    return sum/len(table)

def findcount(table,agg_list,index):
    return len(table)

def after_distinct(all_table,col_dist,col_projection):
    # print(all_table)
    set1={}
    set1=set()
    flag_x=False
    # print(meta_data)
    for i in col_projection:
        # for k in meta_data:
        #     value=meta_data[k]
        #     for v in value:
        #         print("v",v)
        #         print("I",i)
        #         if v==i:
        #             flag_x=True
        #             print("print true")
        #             break
        # if flag_x==False:

        #     print("There is no column present in database named",i)
        #     exit()
        if i.lower() not in col_table_mapping:
            print()
            print("There is no column present in database named",i)
            exit()
        print(col_table_mapping[i.lower()]+"."+i,end="\t")
    print()
    for i in range(len(all_table)):
        tmp_list=[]
        for j in col_dist:
            tmp_list.append(all_table[i][j])
        if tuple(tmp_list) not in set1:
            set1.add(tuple(tmp_list))
            for i in range(len(tmp_list)):
                print(tmp_list[i],end="\t")
            print()
            # print(tmp_list


def after_group_by(col_group_by_index,all_table,aggr_list):
    all_table=sorted(all_table,key=lambda x: int(x[col_group_by_index[0]]))
    an_iterator = itertools.groupby(all_table, lambda x : x[col_group_by_index[0]]) 
    # print(all_table)
    # print(col_group_by_index[0])
    ans_table=[]
    for key, group in an_iterator:
        tmp_table=[]
        # key_and_group = {key : list(group)} 
        # print("Key",key)
        tmp_table=all_table[0]
        tmp_table[col_group_by_index[0]]=key
        # tmp_table.insert(col_group_by_index[0],key)
        # print("tm_table",tmp_table)
        list_group=list(group)
        for i in range(len(aggr_list)):
            if aggr_list[i][0].lower()=="max":
                col_n=[]
                col_n.append(aggr_list[i][1])               
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)

                tmp=findmax(list_group,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp

                # print(col_index[0],tmp)
            elif aggr_list[i][0].lower()=="min":
                col_n=[]
                col_n.append(aggr_list[i][1])                
                # print(col_n)
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)

                tmp=findmin(list_group,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp

            elif aggr_list[i][0].lower()=="sum":
                col_n=[]
                col_n.append(aggr_list[i][1])
                # print(col_n)
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)

                tmp=findsum(list_group,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp

                # print(col_index[0],tmp)
            elif aggr_list[i][0].lower()=="avg":
                col_n=[]
                col_n.append(aggr_list[i][1])
                   # print(col_n)
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)

                tmp=findavg(list_group,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp

            elif aggr_list[i][0].lower()=="count":
                col_n=[]
                col_n.append(aggr_list[i][1])
                # print(col_n)
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)

                tmp=findcount(list_group,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
        # print(tmp_table)
        # print("Final table before append",ans_table)
        # ans_table.append(tmp_table)
        ans_table.append(list(tmp_table))
        # print("Final table",ans_table)

        # print(list(group)) 
    # print(final_table)
    return ans_table

def after_orderby(all_table,col_order_by_index):
    # print("all table",all_table)
    # print("order by index",col_order_by_index)
    if order_desc==True:
        # print("IN DESC ORDER")
        all_table=sorted(all_table,key=lambda x: int(x[col_order_by_index[0]]),reverse=True)
    else:
        all_table=sorted(all_table,key=lambda x: int(x[col_order_by_index[0]]))

    return all_table


def after_where(all_table, table_name, con_operation, condition):
    final_table = []
    row = len(all_table)
    col = len(all_table[0])

    col_index = get_col_index(table_name, con_operation)

    for i in range(int(len(con_operation)/3)):
        # print("Pujan",isinstance(col_index[3 * i + 0],int))
        if isinstance(col_index[3 * i + 0],int) == False:
            print("Where clause has syntax error")
            exit()

        if check_operator(str(col_index[3 * i + 1])) == False:
            print("Not valid Operator : " + col_index[3 * i + 1])
            exit()

        if str(col_index[3 * i + 1]) == "=":
            col_index[3 * i + 1] = "=="

    for i in range(row):
        result = []
        for j in range(int(len(con_operation)/3)):
            if con_operation[3 * j + 2] == col_index[3 * j + 2]:
                s = str(all_table[i][col_index[3 * j + 0]]) + str(col_index[3 * j + 1]) + str(col_index[3 * j + 2])
                result.append(eval(s))
            else:
                s = str(all_table[i][col_index[3 * j + 0]]) + str(col_index[3 * j + 1]) + str(all_table[i][col_index[3 * j + 2]])
                result.append(eval(s))	
        # print(result)
        if condition == "AND":
            flag = result[0] and result[1]
        elif condition == "OR":
            flag = result[0] or result[1]
        else:
            flag = result[0]
        
        if flag:
            final_table.append(all_table[i])
    # print(final_table)
             

    return final_table
  

def query_parse(input):
    
    global where_con
    global col_order_by
    global col_group_by
    global col_projection
    global table
    global flag_select
    global flag_from
    global flag_where 
    global flag_group_by
    global flag_order_by
    global flag_distinct
    global flag_is_star
    global order_desc
    input=sqlparse.parse(str(input))[0]
    # print(input.tokens)
    for item in input:
        if(item.is_keyword):
            if(item.value.upper()=="SELECT"):
                flag_select=True
            elif(item.value.upper()=="FROM"):
                flag_from=True
            elif(item.value.upper()=="GROUP BY"):
                flag_group_by=True
            elif(item.value.upper()=="ORDER BY"):
                flag_order_by=True
            elif(item.value.upper()=="DISTINCT"):
                flag_distinct=True
        # elif(item.is_wildcard):
        #     flag_is_star=True
        elif(item.is_whitespace):
            continue
        elif(isinstance(item,sqlparse.sql.Where)):
            # for i in item:
            #     if(i.value != "WHERE" and not i.is_whitespace and i.value != ";"):
            #         where_con.append(i)
            # print(where_con)
            # seen_where = True
            where_con.append(item.value)
        elif (isinstance(item,sqlparse.sql.Function)):
            # print("PUJAN")
            str1=item.value[:-1]
            str1=str1.split('(')
            aggr_list.append(str1)
            # print(str1)
            if str1[1]=="*" or str1[1]=="":
                print("Invalid column name in agg. Function")
                exit()
            # print(str1)
        elif isinstance(item, IdentifierList):
            for identifier in item.get_identifiers():
                if flag_order_by:
                    col_order_by.append(identifier.value.replace('"', ''))
                elif flag_group_by:
                    col_group_by.append(identifier.value.replace('"', ''))
                elif flag_from:

                    table.append(identifier.value.replace('"', ''))
                elif (isinstance(identifier,sqlparse.sql.Function)):
                    str1=identifier.value[:-1]
                    str1=str1.split('(')
                    aggr_list.append(str1)
                    if str1[1]=="*" or str1[1]=="":
                        print("Invalid column name in agg. Function")
                        exit()
                elif flag_select:
                    col_projection.append(identifier.value.replace('"', ''))
            # print("agg_list",aggr_list)
        elif isinstance(item, Identifier):
            if flag_order_by:
                str1=item.value
                str1=str1.split(" ")
                if len(str1)>1:

                    if str1[1].lower() == "desc":
                        order_desc=True
                col_order_by.append(str1[0])
                # print("col_orderby",col_order_by)

            elif flag_group_by:
                col_group_by.append(item.value)
            elif flag_from:
                table.append(item.value)
            elif flag_select:
                col_projection.append(item.value)
    # print(flag_group_by)
    # print("Distinct",flag_distinct)
    # print(col_projection)
    # print(table)
    # print(col_group_by)
    # print(col_order_by)






def main():
    # print(sys.argv[1][-1])
    if(sys.argv[1][-1]!=";"):
        print("; is missing at the end of given query")
        exit()
    get_metadata()
    # print(meta_data['table1'])
    ip=sys.argv[1].lower()
    query_parse(ip)
    # print("table",table)
    all_table=get_table(table)
    # print(all_table)
    col_projection_index=get_col_index(table,col_projection)
    # print("projection colums ",col_projection_index)
    col_group_by_index=get_col_index(table,col_group_by)
    # print("groupby colums ",col_group_by_index)
    col_order_by_index=get_col_index(table,col_order_by)
    # print("orderby colums ",col_order_by_index)
    if len(where_con) > 0:

        str1=where_con[0]
        str1=str1[:-1]
        str1=str1.replace("where ","")
        condi=str1.split(" ")
        condition="NO"
        if re.search('and',str1,re.IGNORECASE):
            condition="AND"
            str1=str1.split()
            del str1[3]
        elif re.search('or',str1,re.IGNORECASE):
            condition="OR"
            str1=str1.split()
            del str1[3] 
        else:
            str1=str1.split()
        all_table=after_where(all_table,table,str1,condition)
    # print("After Where Table",all_table)
    flag_agg=False
    if len(aggr_list) >0 :
        for i in range(len(aggr_list)):
            col_projection.append(aggr_list[i][1])
            col_projection_index=get_col_index(table,col_projection)
    if len(col_group_by_index) > 0:
        flag_agg=True
        all_table = after_group_by(col_group_by_index,all_table,aggr_list)

    if flag_agg==False and len(aggr_list)>0:
        tmp_table=all_table[0]
        final_table=[]
        for i in range(len(aggr_list)):
            if aggr_list[i][0].lower()=="max":
                # col_n=list(aggr_list[i][1])
                col_n=[]
                col_n.append(aggr_list[i][1])
                # print(col_n)
                # print(table)
                col_index=get_col_index(table,col_n)
                # print(col_index)
                # print("COLUMN NAME",col_n)
                # print("COLUMNaggr_list[i][1] INDEX",col_index)
                # print("PUJAN")            
                # print("COLUMN NAME",col_n)

                tmp=findmax(all_table,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
                        # print(col_index[0],tmp)
            elif aggr_list[i][0].lower()=="min":
                col_n=[]
                col_n.append(aggr_list[i][1])                    
                # print(col_n)
                        # print(table)
                col_index=get_col_index(table,col_n)
                        # print(col_index)

                tmp=findmin(all_table,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
            elif aggr_list[i][0].lower()=="sum":
                col_n=[]
                col_n.append(aggr_list[i][1])
                        # print(col_n)
                        # print(table)
                col_index=get_col_index(table,col_n)
                        # print(col_index)

                tmp=findsum(all_table,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
            elif aggr_list[i][0].lower()=="avg":
                col_n=[]
                col_n.append(aggr_list[i][1])
                        # print(col_n)
                        # print(table)
                col_index=get_col_index(table,col_n)
                        # print(col_index)

                tmp=findavg(all_table,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
            elif aggr_list[i][0].lower()=="count":
                col_n=[]
                col_n.append(aggr_list[i][1])
                        # print(col_n)
                        # print(table)
                col_index=get_col_index(table,col_n)
                        # print(col_index)

                tmp=findcount(all_table,aggr_list,col_index[0])
                tmp_table[col_index[0]]=tmp
        final_table.append(tmp_table)
        tmp_table=[]
        all_table=final_table
        # print("Final Table",all_table)

    if len(col_order_by) >0:
        all_table=after_orderby(all_table,col_order_by_index)

  
    if flag_distinct==True:
        after_distinct(all_table,col_projection_index,col_projection)
    else:
        get_table_data_byindex(all_table,col_projection_index)


if __name__ == "__main__":
	main()


