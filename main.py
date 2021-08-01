import mysql.connector
from datetime import datetime

# The schema of every country table
schema = ["Name VARCHAR(255) PRIMARY KEY NOT NULL","Customer_Id VARCHAR(18) NOT NULL",
"Open_Dt DATE NOT NULL","Consul_Dt DATE","VAC_Type CHAR(5)","Doc_Name CHAR(255)","State CHAR(5)",
"Country CHAR(5)","DOB DATE","Active_Flag CHAR(1)"]

# Initial connection to mysql db
def connect_to_db():
	mydb = mysql.connector.connect(
	host="localhost",
	user="root",
	password="",
	database="Incubyte_Test"
	)

	conn = mydb.cursor()
	conn.execute("CREATE DATABASE IF NOT EXISTS Incubyte_Test")
	return mydb,conn

# Inserting the dictionary values into database tables
def populate_country_tables(conn,dataset):
	try:
		for country,value in dataset.items():
			for data in value:
				query = "INSERT INTO " + country + "(Name, Customer_Id, Open_Dt, Consul_Dt, VAC_Type, Doc_Name, State, Country, DOB, Active_Flag) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
				
				params = (data[index_dict["Customer_Name"]], data[index_dict["Customer_Id"]], 
					data[index_dict["Open_Date"]], data[index_dict["Last_Consulted_Date"]],
					data[index_dict["Vaccination_Id"]], data[index_dict["Dr_Name"]], 
					data[index_dict["State"]], data[index_dict["Country"]],
					data[index_dict["DOB"]], data[index_dict["Is_Active"]])
				
				conn.execute(query,params)

	except Exception as err:
		print("Error in inserting into table: Error: " + str(err))

# Used to print contents of all country tables
def print_country_tables(conn,countries):
	try:
		for country in countries:
			print("DATA FOR COUNTRY->" + country)
			conn.execute("SELECT * FROM " + country)
			cursor = conn.fetchall()
			for row in cursor:
				for values in row:
					print(str(values) , end = "\t")
				print()
	except Exception as err:
		print("Error in printing table: Error: " + str(err))

# This function is used to create all the country tables if they dont exist
def create_country_tables(conn,countries):
	try:
		for country in countries:
			values = ",".join(map(str,schema))
			conn.execute("CREATE TABLE IF NOT EXISTS " + country + "(" + values + ")")

	except Exception as err:
		print("Error in creating table: Error: " + str(err))

#This function is used for extracting data from datafile and storing it countrywise in a dictionary
def extract_data(data_file):
	countries=set() # Set used to store all country names
	index_dict = {} # Used to maintain index of all feilds of tables 
	delimiter = "|"
	dataset={} # Used to store the rows of each country, Key here is the country and value is list of rows to be inserted  
	with open(data_file,'r') as fd:
		for line in fd:
			if (line.lstrip(delimiter).split(delimiter)[0] == 'H'):
				splitted_header = line.lstrip(delimiter).strip('\n').split(delimiter)

				#Creating index dictionary so that we dont have to remember indexes of fields in dataset
				index_dict = { value:index for index,value in enumerate(splitted_header) }
			else:
				splitted_data = line.lstrip(delimiter).strip('\n').split(delimiter)
				country = "Table_" + splitted_data[index_dict['Country']]
				countries.add(country)
				
				#Using try except to create an empty list everytime on first occurance of country
				try:
					length=len(dataset[country])
				except KeyError:
					dataset[country]=[]

				#Converting the ddmmyyyy format to yyyymmdd format for DOB feild	
				converted_dob = format_date(splitted_data[index_dict['DOB']])
				splitted_data[index_dict['DOB']] = converted_dob
				dataset[country].append(splitted_data) 
		# print(index_dict)
		# print(countries)
		# print(dataset)
	return countries,index_dict,dataset

#Used to convert ddmmyyyy to yyyymmdd
def format_date(val):
	if val != "":
		date_time_val = datetime.strptime(val,'%d%m%Y')
		converted_val = date_time_val.strftime('%Y%m%d')
		return converted_val
	else:
		return val 

mydb,conn = connect_to_db()
data_file="datafile.txt"
countries,index_dict,dataset = extract_data(data_file)
create_country_tables(conn,countries)
populate_country_tables(conn,dataset)
print_country_tables(conn,countries)
mydb.commit()