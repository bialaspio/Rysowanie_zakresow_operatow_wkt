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
			if 'P.' in path_operat and '_P.' not in path_operat: 
				print operat_baza
				update_query = """COMMENT ON table partia_05."""+'"'+operat_baza+'"'+""" is '01_Nowy';"""
				exec_query_commit_PG(update_query)
				add_tr_coment(operat_baza)
			#os.system("pause")

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def add_tr_coment(operat_baza):
	"""+'"'+operat_baza+'"'+"""
	tr_query = 	"""CREATE OR REPLACE FUNCTION partia_05.dodaj_kom_"""+operat_baza+"""() RETURNS TRIGGER AS $dodaj_kom$
		begin 
			 IF EXISTS(
		       	select 1 from ( 
								select count (status) as licz from( 
									select status  from partia_05."""+'"'+operat_baza+'"'+""" 
								) as foo where status = '04_Zakres_operatu'
				) as foo2 where licz = (select count (status) from partia_05."""+'"'+operat_baza+'"'+""")
		) 						 
			then
			    COMMENT ON table partia_05."""+'"'+operat_baza+'"'+""" is '04_Zakres_operatu'; 
				RETURN null;
		
			elseif EXISTS(
							select 1 from (
								select *from partia_05."""+'"'+operat_baza+'"'+""" where status like '03_Dodany_zakres' 
						) as foo		
			) 						 
				then
					COMMENT ON table partia_05."""+'"'+operat_baza+'"'+""" is '03_Dodany_zakres'; 
					RETURN null;

			elseif EXISTS(
						select 1 from (
								select *from partia_05."""+'"'+operat_baza+'"'+""" where status like '02_Zarezewowany' 
						) as foo		
			) 						 
				then
					COMMENT ON table partia_05."""+'"'+operat_baza+'"'+""" is '02_Zarezewowany';
					RETURN null;    
			
			 ELSE   
				RETURN null;    
			END IF;
		END;
	$dodaj_kom$ LANGUAGE 'plpgsql';"""
	exec_query_commit_PG(tr_query)

	tr_query ="""DROP TRIGGER IF EXISTS dodaj_kom_"""+operat_baza+""" ON """+'"'+operat_baza+'"'+""";"""
	exec_query_commit_PG(tr_query)

	tr_query ="""CREATE TRIGGER dodaj_kom_"""+operat_baza+"""
				AFTER UPDATE 
					ON partia_05."""+'"'+operat_baza+'"'+"""
				   FOR EACH ROW
				EXECUTE PROCEDURE partia_05.dodaj_kom_"""+operat_baza+"""();"""

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