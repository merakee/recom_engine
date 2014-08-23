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
require 'sqlite3'


class SqliteApi
	def initialize(filename)
		@db = SQLite3::Database.new(filename)
	end

	def close_db
		@db.closed? || @db.close
	end

	def create_table(table_name:, col_def:, drop: false)
		# Example: 
		# col_def: (word TEXT NOT NULL UNIQUE,
	 	#        	  count INT NOT NULL)
		if drop
			sql = "DROP TABLE IF EXISTS #{table_name}"
			@db.execute(sql)
			sql = "CREATE TABLE #{table_name} #{col_def}"
			@db.execute(sql)
		else
			sql = "CREATE TABLE IF NOT EXISTS #{table_name}  #{col_def}"
			@db.execute(sql)

		end
	end


	def execute_sql(sql)
		 @db.execute(sql)
	end

	def get_one(sql)
		@db.get_first_row(sql)
	end
end

# commend line option
if ARGV[0] && ARGV[0]=='test'
	# run test code
	db = SqliteApi.new("test.sqlite")
	table_name = "test_table"
	col_def = <<SQL 
			(user_id INTEGER NOT NULL,
			 tagline TEXT NOT NULL,
			 score REAL NOT NULL)				
SQL
	db.create_table(table_name: table_name, col_def: col_def, drop: true)
	user_id=13
	tagline = "This is the tag line for user"
	score= 23.4
	sql = "INSERT INTO #{table_name} (user_id,tagline,score) VALUES (#{user_id},'#{tagline}',#{score})"
	db.execute_sql(sql)
	sql = "SELECT * FROM #{table_name} WHERE user_id=#{user_id}"
	res = db.get_one(sql)
	if (res[0] != user_id) or (res[1] != tagline) or (res[2]- score).abs>0.0001
		print "Error: the results do not match"
	end

	db.close_db

end

