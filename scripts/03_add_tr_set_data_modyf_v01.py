#
# Small script to show PostgreSQL and Pyscopg together
#

import sys, os, glob, time, datetime, errno, psycopg2
import ntpath, shutil
from datetime import datetime

print sys.stdout.encoding
os.system("pause")

def exec_query_PG ( str_query ):
	try:
		cur_PG.execute(str_query) 
#		print "Wykonane zapytanie "+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		err_log.write('Nie udalo sie wykonac zapytania (select):'+str_query+'\n')
		print e
	return cur_PG

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~
def exec_query_commit_PG( str_query ):
	try:
		cur_PG.execute(str_query) 
		conn_PG.commit()
#		print "Wykonane zapytanie "+str_query
	except Exception, e:
		print '	Nie udalo sie wykonac zapytania:'+str_query
		print e
		err_log.write('Nie udalo sie wykonac zapytania (commit):'+str_query+'\n')
		os.system("pause")
	return cur_PG 	

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~
def pobierz_operaty(path):
	#print path 
#for p, d, f in os.walk(dir):
#		for file in f:
#			if file.endswith('.pdf') or file.endswith('.PDF'):
	operaty = os.listdir(path)
	for operat in operaty:
		operat_baza = operat.replace(".", "_", 3)
		path_operat = path+'\\'+operat+'\\'	
		
		if os.path.isdir(path_operat):
			if 'P.' in path_operat: 
				print operat_baza
				
				set_data_modyf_(operat_baza)
			#os.system("pause")

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def set_data_modyf_(operat_baza):
	#"""+'"'+operat_baza+'"'+"""
	tr_query = 	"""CREATE OR REPLACE FUNCTION partia_05.set_data_modyf_"""+operat_baza+"""() RETURNS TRIGGER AS $set_data_modyf$
    BEGIN
        IF (TG_OP = 'UPDATE') AND NEW.status <> OLD.status THEN
			update partia_05."""+'"'+operat_baza+'"'+""" set data_modyf = now() where id = NEW.id;
            RETURN NULL;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$set_data_modyf$ LANGUAGE plpgsql;"""

	
	exec_query_commit_PG(tr_query)

	tr_query ="""CREATE TRIGGER set_data_modyf_"""+operat_baza+"""
				AFTER INSERT OR UPDATE 
					ON partia_05."""+'"'+operat_baza+'"'+"""
				   FOR EACH ROW
				EXECUTE PROCEDURE partia_05.set_data_modyf_"""+operat_baza+"""();"""

	exec_query_commit_PG(tr_query)
	
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
##############################################################################################################################################################
#-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-
	
if not os.path.exists('.\\log'):
	try:
		os.makedirs('.\\log')
	except OSError as exc: # Guard against race condition
		if exc.errno != errno.EEXIST:
			raise

naz_lik_log = time.strftime("%Y%m%d-%H_%M_%S")
naz_log = open('.\\log\\copy'+naz_lik_log+'.log', 'w')
err_log = open('.\\log\\ERR'+naz_lik_log+'.log', 'w')


#polacznie z baza POSTGRES  
teraz = time.asctime( time.localtime(time.time()))
try:
	#produkcja
	conn_PG = psycopg2.connect("dbname='__dbname__' user='__user__' host='__host__' password='__passwd__' port='5432'")
	naz_log.write(teraz + ' [INF] Polaczono z baza danych\n')
except:
	print "I am unable to connect to the database"
	err_log.write(teraz +' - [ERR] Nie udalo sie naiazac polacznia z baz\n')

cur_PG = conn_PG.cursor()

pobierz_operaty ('k:\\Bierunsko_Ledzinski_cz3\\03_skany\\partia5\\')

conn_PG.close()
print'koniec programu'
os.system("pause")
