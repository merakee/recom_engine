#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

#  Bijit Halder
#  Created: 16 Aug 2014  
#  Revision History: 
#



Dir.glob("#{ENV['MAHOUT_DIR']}/libexec/*.jar").each { |d| require d }

thershold =3.0

MahoutFile = org.apache.mahout.cf.taste.impl.model.file
model = MahoutFile.FileDataModel.new(java.io.File.new("ratings_bool.csv"))

MahoutSimilarity = org.apache.mahout.cf.taste.impl.similarity
#similarity = MahoutSimilarity.EuclideanDistanceSimilarity.new(model)
similarity = MahoutSimilarity.PearsonCorrelationSimilarity.new(model)
similarity = MahoutSimilarity.UncenteredCosineSimilarity.new(model)
#similarity = MahoutSimilarity.SpearmanCorrelationSimilarity.new(model)
#similarity = MahoutSimilarity.LogLikelihoodSimilarity.new(model)
#similarity = MahoutSimilarity.TanimotoCoefficientSimilarity.new(model)

neighborhood_size=100
MahoutNeighborhood = org.apache.mahout.cf.taste.impl.neighborhood
neighborhood = MahoutNeighborhood.NearestNUserNeighborhood.new(neighborhood_size, similarity, model)

MahoutRecommender = org.apache.mahout.cf.taste.impl.recommender
#recommender = MahoutRecommender.GenericBooleanPrefUserBasedRecommender.new(model, neighborhood, similarity)
recommender = MahoutRecommender.GenericUserBasedRecommender.new(model, neighborhood, similarity)
user_id = 4169
recommendations_size=100
recommendations = recommender.recommend(user_id, recommendations_size)

for rec in recommendations
	item_id = rec.get_item_id 
	etimates = recommender.estimatePreference(user_id,item_id)
	etimates_b = (etimates > thershold)?5:1
	puts "#{rec}: #{etimates} #{etimates_b}"
end