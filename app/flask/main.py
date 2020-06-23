

# Import libraries
import os
import sys
import config
import psycopg2
import pandas as pd
from datetime import datetime
from urllib.parse import quote_plus

from flask_wtf import Form
from wtforms.fields import SelectField
from wtforms.fields.html5 import DateField
from wtforms.validators import DataRequired, ValidationError

from flask import request
from flask_bootstrap import Bootstrap
from flask import render_template,Flask, Response
from flask_paginate import Pagination, get_page_parameter, get_page_args


# Configure app
app = Flask(__name__)
app.debug = True
Bootstrap( app )

# Create a random secret key
SECRET_KEY = os.urandom(32)
app.config['SECRET_KEY'] = SECRET_KEY

# Create a url encoder for jinja
app.jinja_env.filters['quote_plus'] = lambda u: quote_plus(u)

# Configure DB connection
user = config.postgres["USER"]
host = config.postgres["HOST"]
port = config.postgres["PORT"]
dbname = config.postgres["DB"]
password = config.postgres["PASS"]
db_conn = psycopg2.connect( host = host, port = port, database = dbname, user = user, password = password )

# Create categories 
cat_choice = ["All"]
cat_agora = cat_choice + ["Counterfeits", "Data", "Drug", "Electronics", "Forgeries", "Info", "Jewelery", "Services", "Tobacco", "Weapon", "Other"]	
cat_silkroad = cat_choice + ["Apparel", "Art", "Book", "Custom-orders", "Digital", "Drug", "Electronics", "Erotica", "Forgeries", "Herbs", "Jewelery", "Lab-supplies", "Games", "Medical", "Money", "Packaging", "Services", "Writing"]

# Title of the page
t_title = "darkcoin"

# Default query when the page first loads
query_results = pd.read_sql_query( "SELECT ( product_name ), price, vendor, ad_date, category from acc_transac limit 99;", db_conn )




class dark_coin_search( Form ):
	"""
	Form for Filtering

	"""
	
	# Create DateFields for date filtering
	dt1 = DateField( format = '%Y-%m-%d', default = datetime.strptime( '2014-01-01', '%Y-%m-%d' ), validators = [DataRequired()], render_kw = { 'onkeyup':"saveValue(this)", 'onload':"getSavedValue(this.id)" } )
	dt2 = DateField( format = '%Y-%m-%d', default = datetime.strptime( '2014-02-01', '%Y-%m-%d' ), validators = [DataRequired()], render_kw = { 'onkeyup':"saveValue(this)", 'onload':"getSavedValue(this.id)" } )
	
	# Create SelectFields for picking marketplace and categories to filter
	marketplace = SelectField( u'Marketplace', choices = ["All", 'Agora', 'Silkroad2', 'CryptoMarket'], default = 1, validators = [DataRequired()], render_kw = { 'onchange': "chooseCat();saveValue(this)", 'onload':"getSavedValue(this.id)" } )
	
	# Default category - All
	category = SelectField( u'Category', choices = cat_choice, validators = [DataRequired()], render_kw = { 'onchange':"saveValue(this)", 'onload':"getSavedValue(this.id)" } )
	
	# Categories of silkroad2
	category_silkroad = SelectField( u'Category', choices = cat_silkroad, validators = [DataRequired()], render_kw = { 'onchange':"saveValue(this)", 'onload':"getSavedValue(this.id)" } )
	
	# Categories of agora
	category_agora = SelectField( u'Category', choices = cat_agora, validators = [DataRequired()], render_kw = { 'onchange':"saveValue(this)", 'onload':"getSavedValue(this.id)" } )




def chunk_results( offset = 0, per_page = 10, query_results = [] ):
	"""
	Partition query_results for pagination

	Parameters:
	offset (int): offset of the pagination
	per_page (int): number of results that will be shown in each page
	query_results (table): data table that will be divided into chunks

	Returns:
	pandas table: chunk of the pandas table that will be displayed on the current page

	"""
	# Return the chunk of rowns starting from the offset 
	return query_results.iloc[ offset : offset + per_page, : ]




@app.route( "/", methods = ( 'GET', 'POST' ) )
def main():
	"""
	Main page - results.html

	"""

	global query_results

	# Form field results
	date1 = None	# Starting Date
	date2 = None	# Ending Date
	marketplace = None	# Chosen Dark Web market
	category = None		# Chosen category

	# If form has been submitted
	submitted = False

	# Create the form
	form = dark_coin_search()


	page = request.args.get( get_page_parameter(), type = int, default = 1 )
	page, per_page, offset = get_page_args( page_parameter = 'page',
                                           per_page_parameter = 'per_page' )

	# If it is the first page of the pagination
	if ( page < 2 ) or page == None:

		# If form is submitted
		if form.is_submitted():
			submitted = True 

			# Get selected values from the form fields
			date1 = form.dt1.data.strftime( '%Y-%m-%d' )
			date1 = str( date1 )
			date2 = form.dt2.data.strftime( '%Y-%m-%d' )
			date2 = str( date2 )

			marketplace = form.marketplace.data.lower()

			category = form.category.data.lower()

			if marketplace == "silkroad2":
				category = form.category_silkroad.data.lower()

			elif marketplace == "agora":
				category = form.category_agora.data.lower()

	    
		# s string to hold select query
		s = ""

		# If the form is submitted and not a specific marketplace was selected
		if ( submitted == True and marketplace == 'all' ):
			s = "SELECT DISTINCT( product_name ), price, vendor, ad_date, category FROM acc_transac WHERE ( time >= \'{}\' AND time <= \'{}\' ) ORDER BY ad_date;".format( date1, date2 )
		
		# If the form is submitted, a specific marketplace was selected but category was not selected 
		elif ( submitted == True and marketplace != 'all' and category == 'all' ):
			s = "SELECT DISTINCT( product_name ), price, vendor, ad_date, category FROM acc_transac WHERE ( time >= \'{}\' AND time <= \'{}\' AND marketplace = \'{}\' ) ORDER BY ad_date;".format( date1, date2, marketplace )
		
		# If the form is submitted, a specific marketplace and category was selected 
		elif ( submitted == True and marketplace != 'all' and category != 'all' ):
			s = "SELECT DISTINCT( product_name ), price, vendor, ad_date, category FROM acc_transac WHERE ( time >= \'{}\' AND time <= \'{}\' AND marketplace = \'{}\' AND category ILIKE \'%{}%\' ) ORDER BY ad_date;".format( date1, date2, marketplace, category )

		# If form is submitted read sql query from the DB
		if ( submitted == True ):
			query_results = pd.read_sql_query( s, db_conn )


	# Get chunk of the data
	pagination_results = chunk_results( offset, per_page, query_results )

	# Create pagination
	pagination = Pagination( page = page, per_page = per_page, total = query_results.count()[0], search = False, record_name = 'query_results', css_framework = 'bootstrap4' )
    
    # Return parameters to the page
	return render_template( "results.html", t_title = t_title, page = page, offset = offset, per_page = per_page, query_results = pagination_results,  len = len(pagination_results), form = form, pagination = pagination );




@app.route( "/details", methods = ( 'GET', 'POST' ) )
def details():
	"""
	Details page - item.html

	"""

	# Read parameters from the url
	pname = request.args.get( 'item' )
	pdate = request.args.get( 'date' )

	# Select query for getting the transactions of the product given
	s = "SELECT * FROM acc_transac WHERE ( product_name = \'{}\' AND ad_date = \'{}\' ) Limit 99;".format( pname, pdate )
	query_results = pd.read_sql_query( s, db_conn )
	
	# Return parameters to the page
	return render_template( "item.html", t_title = t_title, query_results = query_results, len = len( query_results ) );




if __name__ == '__main__':
	app.run()


