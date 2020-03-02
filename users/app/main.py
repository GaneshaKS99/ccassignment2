from flask import Flask, render_template,\
jsonify,request,abort, Response
import json
import requests
from datetime import datetime
app=Flask(__name__)

import sqlite3

#@app.route('/write/)
#def write(username,password):
@app.route('/api/v1/db/read',methods=['POST'])
def read_db():
    req=request.get_json()

    table= req.get("table")
    columns= req.get("columns")
    where= req.get("where")
	
		
    final_column=''
    for i in columns:
        final_column+= i+', '

    final_column=final_column[:-2]
	
    query=''
    if len(where)==0:
        query="SELECT "+final_column+" FROM "+table+";"

    else:
        final_where=''
        for i in where:
            final_where+= i+' and '
		
        final_where= final_where[:-5]
        query='SELECT '+final_column+' FROM '+table+' WHERE '+ final_where+';'
    res={}
    #print(query)
    try:
        conn=sqlite3.connect("user.db")
        c= conn.cursor()

        check= c.execute(query).fetchall()
        #print(check)
        count = len(check)
        #print("COUNT", count)
        #res={}
        res['count']=count
        #print("COUNT res",res["count"])
        column_index={}
        for i in range(len(columns)):
            column_index[i]=columns[i]
        #print(column_index)
        for i in columns:
            res[i]=[]

        for i in range(count):
            for j in range(len(check[i])):
                res[column_index[j]].append(check[i][j])
        
        
        res['status']=200
        print(res)
        conn.close()
        return json.dumps(res)
    
    except Exception as err:

        print(err)
        res['status']=400
        conn.close()
        return json.dumps(res)
    
    finally :
        if conn:
            conn.close()
        else:
            pass


@app.route('/api/v1/db/write',methods=['POST'])
def write_db():
    req=request.get_json()
    table=req.get("table")
    flag=req.get("flag")
    query=''
    if flag==0:     #INSERT
        values=req.get("values")
        columns=req.get("columns")
        final_column=''
        for i in columns:
            final_column+="'"+i+"'"+', '
        final_column=final_column[:-2]
        final_values=''
        for i in values:
            final_values+="'"+i+"'"+', '
        final_values=final_values[:-2]

        query='INSERT INTO '+table+' ('+final_column+') VALUES ('+final_values+');'
    
    else:
        cond=req.get("condition")
        if len(cond)==0:
            query='DELETE FROM '+table+' ;'
        else:
            final_cond=''
            for i in cond:
                final_cond+=i+' and '
            final_cond=final_cond[:-5]

            query='DELETE FROM '+table+' WHERE '+final_cond+";"
    
    print(query)
    res={}
    try:
        conn=sqlite3.connect("user.db")
        c= conn.cursor()
        q="PRAGMA foreign_keys=OFF"
        c.execute(q)
        conn.commit()
        q="PRAGMA foreign_keys=ON"
        c.execute(q)
        conn.commit()
        try:
            c.execute(query)
            conn.commit()
            res["count"]=1
            res["status"]=200
            conn.close()
            return json.dumps(res)
        except Exception as e:
            print("HERE1")
            print(e)
            res["count"]=0
            res["status"]=400
            conn.close()
            return json.dumps(res)

    except Exception as err:
        print("HERE")
        print(err)
        res["count"]=0
        res["status"]=400
        conn.close()
        return json.dumps(res)
        
    finally :
        if conn:
            conn.close()
        else:
            pass




@app.route('/api/v1/users',methods=['GET'])
def userss():
    r= requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/read',
    json={
        "table":"User",
        "columns":["username"],
        "where":[]
    })
    count= r.json().get("count")
    if count>0:
        users= r.json().get("username")
        return json.dumps(users),200
    else:
        return json.dumps({}),204

@app.route('/api/v1/db/clear',methods=['POST'])
def cleardb():
    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/write',
    json={
        "table":"User",
        "flag":1,
        "condition":[]
    })

    count1=r.json().get("count")
    if count1>0:
        return json.dumps({}),200
    else:
        return json.dumps({}),400	#used because its given







@app.route('/api/v1/users',methods=['PUT'])
def Add_user():
    
    req= request.get_json()
    uname= req.get("username")
    print(uname)
    password= req.get("password")

    if(uname=="" or password==""):
       # Response(status=400)
        return json.dumps({}),400

    if len(password)!=40:
       # Response(status=400)
        return json.dumps({}),400    
    try:
        sha=int(password,16)
    except ValueError:
       # Response(status=400)    #return response message as invalid password
        return json.dumps({}),400



    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/read',
    json={
        'table':'User',
        'columns':['username'],
        'where':["username='"+uname+"'"]

    })

    count=r.json().get("count")
    if count>0:
        status_code=r.json().get("status")
        #print(count)
        #res = {}
        #res["count"] = count
        #res["username"]=r.json().get("username")

       # Response(status=400)      #return username already exists
        return json.dumps({}),400
        
        #return json.dumps(res)
    else:
        r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/write',
        json={
            'table':'User',
            'flag':0,
            'columns':['username','password'],
            'values':[uname,password]

        })

        status_code=r.json().get("status")
        count=r.json().get("count")
        res={}
        print(count)
        if count==1:
            res["count"]=count
            #res["username"]=r.json().get()
            #print("COUNT",count)
            
            #Response(status=201)
            return json.dumps({}),201
        
        else:
            #res["count"]=count
           # Response(status=400)   #return couldn't be created
            #return json.dumps(res)
            return json.dumps({}),400


@app.route('/api/v1/users/<username>',methods=['DELETE'])
def REMOVE_user(username):

    r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/read',
    json={
        'table':'User',
        'columns':['username'],
        'where':["username='"+username+"'"]
    })
    
    count=r.json().get("count")
    if count>0:
        r=requests.post('http://ec2-3-222-255-47.compute-1.amazonaws.com:8080/api/v1/db/write',
        json={
            'table':'User',
            'flag':1,
            'condition':["username = '"+username+"'"]
        })
        status_code=r.json().get("status")
        count=r.json().get("count")
       # Response(status=200)
        return json.dumps({}),200



    else:
        #status_code=r.json().get("status")
        #if status_code==200:
         #   return Response(status=400)
       # Response(status=400)    #return username doesn't exist
        return json.dumps({}),400



if __name__ == '__main__':	
	app.debug=True
	app.run(host="0.0.0.0")
