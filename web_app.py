from flask import Flask,request,render_template
from flask_wtf import *

from wtforms import TextField, BooleanField, TextAreaField, StringField, PasswordField, SelectField, validators,FieldList,FormField

#from flask.ext.wtf import Form

app = Flask(__name__)

import json
import subprocess
path = "/sk-users/francesco/spark-1.5.2-bin-2.7.1/app_v_3_0/conf/tmp_conf.txt"

pathspark = "/sk-users/francesco/spark-1.5.2-bin-2.7.1/app_v_3_0/run_pipeline"

@app.route("/api",  methods=['GET', 'POST'])
def api():
	if request.method == 'POST':
		mydata =  request.json
		with open(path, 'w') as outfile:
			json.dump(mydata, outfile)
		rc = subprocess.call(["bash", pathspark , path, mydata.get("name_job", "REST_API")])
		return "COMPLETED"
	else:
		return "Send JSON POST request"


@app.route('/form',  methods=['GET', 'POST'])
def form():
	user_addresses = [{"name": "First Address"},
                  {"name": "Second Address"}]
	form = QueryForm(addresses=user_addresses, csrf_enabled=False)

	if request.method == 'POST':
		text = request.form['name_job']
		print request.form
		form = QueryForm(addresses=user_addresses, csrf_enabled=False)
		return render_template('main.html', form=form)

	return render_template('main.html', form=form)


class QueryEntryForm(Form):
    name = TextField()

class QueryForm(Form):
    """A form for one or more addresses"""
    addresses = FieldList(FormField(QueryEntryForm), min_entries=1)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4048, threaded=True)
