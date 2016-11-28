import smtplib
import os
import codecs
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate



class Email:
	def __init__(self, EMAIL_TO, SUBJECT, sc):
		self.EMAIL_FROM = os.environ["OUTLOOK_EMAIL"]
		self.EMAIL_TO = EMAIL_TO
		self.server = "smtp.office365.com"
		self.SUBJECT = SUBJECT
		self.sc = sc

	def send_mail(self, send_from, send_to, s3_path, query,
                server="smtp.office365.com"):
        #assert isinstance(send_to, list)
		msg = MIMEMultipart()
		msg['From'] = send_from
		msg['To'] = COMMASPACE.join(send_to)
		msg['Date'] = formatdate(localtime=True)
		msg['Subject'] = self.SUBJECT
		text = (u"Email automatically generated by Language Data Pipeline.\n\n" +
				 u"Parameters passed:\n" +
				 str(query) + "\n\n" +
				 u"The query has been saved at: " + s3_path + u"\n\n" +
				 u"This is only a 100 rows sample.\n" +
				 u"For questions please contact Francesco at frferrar@microsoft.com")

		
		try:
			msg.attach(MIMEText(text))
			files = self.load_s3_files_to_csv(s3_path)
			for f in files or []:
			    with open(f, "rb") as fil:
			        part = MIMEApplication(
			           fil.read(),
			           Name=basename(f)
			        )
			        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
			        msg.attach(part)
			smtp = smtplib.SMTP(server)
			smtp.starttls()
			password = os.environ["OUTLOOK_PASSWORD"]
			smtp.login(self.EMAIL_FROM, password)
			smtp.sendmail(send_from, send_to, msg.as_string())

		except (smtplib.SMTPSenderRefused, UnicodeEncodeError) as e:
			msg = MIMEMultipart()
			msg['From'] = send_from
			msg['To'] = COMMASPACE.join(send_to)
			msg['Date'] = formatdate(localtime=True)
			msg['Subject'] = self.SUBJECT
			text = (u"Email automatically generated by Language Data Pipeline.\n\n" +
					 u"The query has been saved at: " + s3_path + u"\n\n" +
					 u"This is only a 100 rows sample.\n" +
					 u"For questions please contact Francesco at frferrar@microsoft.com")

			msg.attach(MIMEText(text + "\n\nPS: Attachment reached maximum size and it has not been attached."))
			smtp = smtplib.SMTP(server)
			smtp.starttls()
			password = os.environ["OUTLOOK_PASSWORD"]
			smtp.login(self.EMAIL_FROM, password)
			smtp.sendmail(send_from, send_to, msg.as_string())

		smtp.close()
		self.remove_tmp_file(files)


	def load_s3_files_to_csv(self, s3_path):
		#data = (self.sc.textFile(s3_path).collect())
		data = (self.sc.textFile(s3_path).take(100))

		tmp_file_path = s3_path.replace("s3a://", "").replace("/", "_")
		f = codecs.open(tmp_file_path, "w", "utf-8")
		for line in data:
			f.write(line + u"\n")
		f.close()
		return [tmp_file_path]

	def remove_tmp_file(self, tmp_file_path):
		for f in tmp_file_path:
			os.remove(f)


