from flask import Flask, request
from flask_restful import Api, Resource
from resource.data_operations import datafetch, tabledata, datacreate, dataread, login, json_to_df,fromdb_todb, call_method, session, logout
from resource.Psg_to_sql import psgr_to_sql
app = Flask(__name__)
api = Api(app)

# This is how you add a resource
api.add_resource(datafetch, '/api/users')
api.add_resource(tabledata,'/api/table')
api.add_resource(datacreate,'/api/create')
api.add_resource(dataread,'/api/read')
api.add_resource(session,'/api/session')
api.add_resource(json_to_df,'/api/jsondf')
api.add_resource(fromdb_todb,'/api/syncdb')
api.add_resource(call_method,'/api/callmethod')
api.add_resource(login,'/api/login')
api.add_resource(logout,'/api/logout')
api.add_resource(psgr_to_sql,'/api/psg_to_sql')


if __name__ == "__main__":
    app.run(debug=True, port=1509)
