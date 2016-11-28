from flask import Flask,request
app = Flask(__name__)
import thread
import json
import subprocess
path = "/app_v_3_0/conf/tmp_conf.txt"
pathspark = "/sk-users/francesco/spark-1.6.1-bin-hadoop-2.7.2/app_v_3_0/run_pipeline"

@app.route("/api",  methods=['GET', 'POST'])
def main():
	if request.method == 'POST':
		mydata =  request.json
		with open(path, 'w') as outfile:
			json.dump(mydata, outfile)
		rc = subprocess.call(["bash", pathspark , path])
		return "Thanks. Your age is %s" % mydata.get("name")
	else:
		return "Hello World!"


def flaskThread():
    app.run(host="0.0.0.0", port=4048)

if __name__ == "__main__":
    thread.start_new_thread(flaskThread,())