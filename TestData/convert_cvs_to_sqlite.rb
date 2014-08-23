#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

# Purpose:: collect feed from various sources and add to the wom back end content
#
#
#
# Coptyright:: Indriam Inc.
# Created By:: Bijit Halder
# Created on:: 28 April 2010
# Last Modified:: 20 July 2014
# Modification History::
#
#
#

# Database set up
require 'csv'
require './sqlite_api.rb'

#====================================
# get dir name
# if(ARGV.length==0)
# 	puts "\n **Error ***: There is no csv file specified. Please specify a file. \n\n"
# 	Process.exit
# end
#csv_filename = "MovieLens/ml-1m/ratings.csv" #ARGV[0]
csv_filename = "MovieLens/ml-10M100K/ratings.csv"

$db =  nil
$table_name =  nil 

def setup_db
	$db =  SqliteApi.new("MovieLensRatings_10M.sqlite")
	$table_name = "ratings_table"
	col_def = <<SQL 
	(user_id INTEGER NOT NULL,
		content_id INTEGER NOT NULL,				
		ratings REAL NOT NULL)				
SQL
$db.create_table(table_name: $table_name, col_def: col_def, drop: true)
end

def insert_data(user_id:,content_id:,ratings:)
	sql = "INSERT INTO #{$table_name} (user_id,content_id,ratings) VALUES (#{user_id},'#{content_id}',#{ratings})"
	$db.execute_sql(sql)
end

setup_db
=begin
CSV.foreach(csv_filename) do |row|
	val = row[0].split("::")[0,3]
	insert_data(user_id: val[0],content_id:val[1],ratings:val[2])
end
=end 
$db.close_db