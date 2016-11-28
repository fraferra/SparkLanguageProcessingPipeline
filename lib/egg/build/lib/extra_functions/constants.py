PATH2GENDER = "s3a://sk-insights/users/francesco/datasets/gender_cloudUserId_2"

PATH2OLDDATA = "sk-model/jobs/MetadataInferenceRerun2016_OldSDK/"

PAH2NEWDATA = "pipeline/augmenter/0/"

STARTING_DATE_NEW_DATA = "2016-02-01"

NEW_DATA_BUCKET = "sk-insights"

POSITIVE_SENTIMENT_DEFAULT_TERMS = ["love","like", "great", ":)", "best"]

NEGATIVE_SENTIMENT_DEFAULT_TERMS = ["horrible","sad","hate","bad","terrible"]

#PATH2WORDVECS = "s3a://sk-insights/users/francesco/vocabs/glove.840B.300d.txt"
PATH2WORDVECS = "s3a://sk-insights/users/francesco/vocabs/glove_vecs_ligth_version.txt"

PATH2INSTALLIDS = "s3a://sk-insights/users/francesco/datasets/installIds_cloudUserIds"

STOPWORDS_1=[u'i', u'me', u'my', u'myself', u'we', u'our', u'ours', u'ourselves', u'you',\
			 u"i'm",u"it's",u"that's", u"u", u"you're",u"i'll", u"ur"\
			 "gonna", "going", "gotta", "got",
			 u"short message",u"know",\
			 u'your', u'yours', u'yourself', u'yourselves', u'he', u'him', u'his', u'himself', u'she',\
			 u'her', u'hers', u'herself', u'it', u'its', u'itself', u'they', u'them', u'their', u'theirs',\
			 u'themselves', u'what', u'which', u'who', u'whom', u'this', u'that', u'these', u'those', u'am',\
			 u'is', u'are', u'was', u'were', u'be', u'been', u'being', u'have', u'has', u'had', u'having', u'do',\
			  u'does', u'did', u'doing', u'a', u'an', u'the', u'and', u'but', u'if', u'or', u'because', u'as',\
			   u'until', u'while', u'of', u'at', u'by', u'for', u'with', u'about', u'against', u'between', u'into',\
			    u'through', u'during', u'before', u'after', u'above', u'below', u'to', u'from', u'up', u'down',\
			     u'in', u'out', u'on', u'off', u'over', u'under', u'again', u'further', u'then', u'once', u'here',\
			      u'there', u'when', u'where', u'why', u'how', u'all', u'any', u'both', u'each', u'few', u'more',\
			       u'most', u'other', u'some', u'such',u'only', u'own', u'same', u'so',\
			        u'than', u'too', u'very', u's', u't', u'can', u'will', u'just', u'don', u'should', u'now']

NEGATIONS = [u'no', u'nor', u'not',u"don't",u"didn't", "aren't","can't","doesn't","weren't","wasn't","couldn't"]
		 
STOPWORDS_2=['india', 'get',\
		'order','name','offer','com','pe','ka','hi','hai',\
	    'check','awb','itm',\
        'first','free','gift','lucky','people','redeem',\
	    'whatsapp','voucher','hurry','se','rs','gp','app',"don't",\
	    'giving','certificate','together','visit','ki','amazonamazon',\
	    'amazonname','one','5,000','18,000','14','-15','-14','3,000','rs5,000',\
	    'lo','flipkartflipkart','download','number','thanks','email','uvbffd',\
	    'naaptolname','9pm','1pm','www','naaptolnaaptol',"i'm",'got',\
	    'ebayebay','flipkartname','shipped','tracking','unsubscribed', 'mailing',\
	    'pvt', 'ltd','id','thank','myntraname','please','50','return','delivery',\
	    'delivered','jabongjabong','ko','myntramyntra', 'infibeaminfibeam', 'infibeamname']

PUNCTUATION = ["!", ":", ",", "'", '"', ";", ":",'.','?','`']

OTHERS=[u'http',u'https',
		u'http://',\
			u'aw',u'gp',u'ref',u'ie',u'cr&ei',u'site',u'search',\
			u'utf',\
            u'short_message',u'bhi',u'tha',u'^',\
            u'http://', u'https://',u'^short_message',\
           u'^name', "it's"]

WEB_SEARCH = [ 'short_message','bhi','tha',\
				'3a','http','https',\
            'http://', 'https://','^short_message',\
            '^name']





STOPWORDS_3=['india', 'get',"it's",\
		'hey','good','really','well','lol',"that's",\
		'net07','hooper','bi','utna','till','mere','liya','want','msg','jabongname','koi','etc','jo',\
		'attempt','address','dear',\
		'apk','buy','ordered','mein','bhai','deliver','main','raha','kuch','ma','nai',\
		'wife','users','working','via','dhamki','dungi','doge','utna','jitna','add','job','contact',\
		'dear','thru',\
		'199','2201',\
		'6400','32','90',\
		"i'll",'ru','2014','18','12','500',\
		'utf8&ref','join-and-earn','encoding','mrp_refl_cp_clbd_ind_snp_lnd&refcust',\
		'karna','aap','chal','ab','hu','placed','fir','aaya','koi','kal','liye','thi','wale',\
		'wala','aur','nahi','abhi','dekh','frm','mai','tum','rha','kiya','sumit',\
		'seller','shipped','delivery','password','click',\
		'tskyv250jbgmzwkn',\
		'999','250','40',\
		'order','name','offer','com','pe','ka','hi','hai',\
		'card','valid','super','details','see','go','list',\
		'shopping','shipment','end','sale','online','coupon','account',\
		'time','shoes','customer','online','coupons',\
		'code','invite',\
		'using','item','itemid',\
	    'check','awb','itm',\
	    'mumbai','taskp30pjbs2khjf','30','snapdealname','tskyv250jbfv3cf4',\
	    'pwd','ebayshort_message','15','203','userid',\
	    'myntrashort_message','pm','nd','jan','2015','80','wo',\
	    '100','25',\
	    'ta','tu','te','ni','ne','er','kr','day','nhi',\
	    'yu','effective','ye','re',':-',\
	    'le','aaj','ho','da','ek','pr','mil','aa',\
	    'ref','aw','also','us','toh',\
	    'back','dp','la','par','send','invitation','link',\
	    '10','use','ke','kar','kya','ur',\
	    'like','try','interview','work','today','site','know','call',\
	    'come','think',\
	    'would','need','sent','give','42c0c5',\
        'first','free','gift','lucky','people','redeem',\
	    'whatsapp','voucher','hurry','se','rs','gp','app',"don't",\
	    'giving','certificate','together','visit','ki','amazonamazon',\
	    'amazonname','one','5,000','18,000','14','-15','-14','3,000','rs5,000',\
	    'lo','flipkartflipkart','download','number','thanks','email','uvbffd',\
	    'naaptolname','9pm','1pm','www','naaptolnaaptol',"i'm",'got',\
	    'ebayebay','flipkartname','shipped','tracking','unsubscribed', 'mailing',\
	    'pvt', 'ltd','id','thank','myntraname','please','50','return','delivery',\
	    'delivered','jabongjabong','ko','myntramyntra', 'infibeaminfibeam', 'infibeamname',\
	    'yes','ya',"won't",'sorry',"'s","'m",'paris','go','walt',"n't",'going', 'california',\
	    'anaheim','na','resort','park','ca']


