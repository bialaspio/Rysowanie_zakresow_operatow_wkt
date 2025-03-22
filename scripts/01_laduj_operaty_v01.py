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
#				print ('UWAGA USUWA !!!!')
				drop_query ='drop table if exists partia_05."'+ operat_baza + '";'
				exec_query_commit_PG(drop_query)
#				os.system("pause")
				print (operat_baza)
				create_query = 'create table partia_05."'+ operat_baza + '"'+""" (
												id serial, 
												plik varchar(32767) NOT NULL,
												operat varchar(32767) NOT NULL,
												uzytkownik varchar NULL,
												status varchar NOT NULL,
												wkt_geometry geometry NULL,
												data_modyf timestamp NOT NULL,
												opr_zakres varchar NULL,
												PRIMARY KEY (id),
												CONSTRAINT fk_username
													FOREIGN KEY(uzytkownik) 
														REFERENCES users(username),
												CONSTRAINT fk_status
													FOREIGN KEY(status) 
														REFERENCES status_operat(status),
												CHECK ((uzytkownik is not null AND status not like '01_Nowy') or (uzytkownik is null AND status like '01_Nowy'))
											)
											WITH (
												OIDS=FALSE
											) ;
											"""				
				exec_query_commit_PG(create_query)

				# rekord dla operatu 
				inseret_query = 'insert into partia_05."'+operat_baza+'" (plik,operat,status,data_modyf)'+" VALUES('operat','"+operat+"','01_Nowy',now());"
				exec_query_commit_PG(inseret_query)
				
				
				files = os.listdir(path_operat)
				
				#rekordy dla poszczegolnych plikow 
				for file in files:
					if file.endswith('.jpg') or file.endswith('.JPG'):
						inseret_query = 'insert into partia_05."'+operat_baza+'" (plik,operat,status,wkt_geometry,data_modyf)'+" VALUES('"+file+"','"+operat+"','01_Nowy','MULTIPOLYGON (((6581730.228609123 5551850.15233694, 6581734.392691579 5551850.045565596, 6581734.392691579 5551850.045565596, 6581734.499462924 5551845.988254485, 6581730.228609123 5551845.988254485, 6581730.228609123 5551850.15233694)))'::geometry,now());"
						#print file
						exec_query_commit_PG(inseret_query)
						
				
				historia(operat_baza)
			#	multipoligony(operat_baza)
				poprawka(operat_baza)
			#os.system("pause")

#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def historia(operat_baza): 
	tr_query = """CREATE OR REPLACE FUNCTION historia_p05() RETURNS TRIGGER AS $emp_audit$
		BEGIN
			--
			-- Create a row in emp_audit to reflect the operation performed on emp,
			-- making use of the special variable TG_OP to work out the operation.
			--
			IF (TG_OP = 'DELETE') THEN
				INSERT INTO partia_05.historia SELECT nextval('partia_05.historia_id_hist_seq'::regclass),'D',now(), OLD.*;
			ELSIF (TG_OP = 'UPDATE') THEN
				INSERT INTO partia_05.historia SELECT nextval('partia_05.historia_id_hist_seq'::regclass),'U',now(), OLD.*;
			END IF;
			RETURN NULL; -- result is ignored since this is an AFTER trigger
		END;
	$emp_audit$ LANGUAGE plpgsql;"""
	exec_query_commit_PG(tr_query)	
	
	tr_query ="""CREATE TRIGGER tr_historia
					AFTER INSERT OR UPDATE OR DELETE 
						ON partia_05."""+'"'+operat_baza+'"'+"""
					   FOR EACH ROW
					EXECUTE PROCEDURE historia_p05();"""
	exec_query_commit_PG(tr_query)
	
#~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~_~

def poprawka(operat_baza): 
	tr_query = """CREATE OR REPLACE FUNCTION partia_05.poprawka_"""+operat_baza+"""() RETURNS TRIGGER AS $poprawka$
		BEGIN
			IF EXISTS(select 1 from ( 
									select count (status) as licz from( 
										select status  from partia_05."""+'"'+operat_baza+'"'+""" where opr_zakres is not null
									) as foo where status = '05_Poprawka'
					) as foo2 where licz > 0)
			 THEN
				-- rekord nie zostanie dodany ani zaktualizowany
				UPDATE partia_05."""+'"'+operat_baza+'"'+""" SET status='04_Zakres_operatu' where status = '05_Poprawka';   
				UPDATE partia_05."""+'"'+operat_baza+'"'+""" set wkt_geometry = (select ST_Union(wkt_geometry) from partia_05."""+'"'+operat_baza+'"'+""" where plik not like 'operat' and opr_zakres = 'TAK'), uzytkownik = 'Admin',status='04_Zakres_operatu' where (plik = 'operat' ) ;
				UPDATE partia_05."""+'"'+operat_baza+'"'+""" SET status='04_Zakres_operatu';   
				RETURN null;
			ELSE
				RETURN null;
			END IF;
		END;
	$poprawka$ LANGUAGE 'plpgsql';"""
	exec_query_commit_PG(tr_query)

	tr_query = """CREATE TRIGGER poprawka_"""+operat_baza+"""
	AFTER UPDATE 
		ON partia_05."""+'"'+operat_baza+'"'+"""
	   FOR EACH ROW
	EXECUTE PROCEDURE partia_05.poprawka_"""+operat_baza+"""();"""
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
#pobierz_operaty (f:\\TMCE\\Bierun\\2023\\rysowanie_wkt\\TEST\\)
conn_PG.close()
print'koniec programu'
os.system("pause")
	