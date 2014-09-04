#!/usr/bin/env ruby -w
# -*- coding: utf-8 -*-

#  Test code for Mahut recommendation engines. 
#  Bijit Halder
#  Created: 16 Aug 2014  
#  Revision History: 
#
require 'csv'
require 'benchmark'

Dir.glob("#{ENV['MAHOUT_DIR']}/libexec/*.jar").each { |d| require d }

# define all vairables
$model =  nil
$similarity =  nil
$neighborhood =  nil
$recommender =  nil
$threshold =3.5
$data_file_name = "ratings_data_bool.csv"
$test_file_name = "ratings_test_bool.csv"
$svd_data_file = nil 
$csv_t = nil 
$similarity_types =%w[Euclidean Pearson UncenteredCosine Spearman LogLikelihood Tanimoto]
$neighborhood_sizes=[50,100,500,1000]
$recommender_types =%w[GenericUserBased GenericBooleanPrefUserBased]
$factorizer = nil
$persistency_policy = nil 
$factorizer_types =%w[ALSWRFactorizer SVDPlusPlusFactorizer ParallelSGDFactorizer  RatingSGDFactorizer]
$lamda_vals =[0.1, 0.05, 0.01]
$num_featues_vals =[50, 100, 200, 500]
$num_iterations_vals =[5, 10, 20]
$recommender_svd_types =%w[SVDRecommender]
$user_ids =[4169,1]

# recommendable 
$recommendable_th=[4.95, 4.9, 4.5, 4.0, 3.5, 3.0, 2.5]
$recommendable_tc=Array.new($recommendable_th.count,0)
$recommendable_ec=Array.new($recommendable_th.count,0)
$recommendable_type_tc=Array.new(2,0)
$recommendable_type_ec=Array.new(2,0)


$logfile = open('performance.log', 'a')

# Mahut
MahoutFile = org.apache.mahout.cf.taste.impl.model.file
MahoutSimilarity = org.apache.mahout.cf.taste.impl.similarity
MahoutNeighborhood = org.apache.mahout.cf.taste.impl.neighborhood
MahoutRecommender = org.apache.mahout.cf.taste.impl.recommender
MahoutRecommenderSVD = org.apache.mahout.cf.taste.impl.recommender.svd

def set_model(filename=$data_file_name)
	$model = MahoutFile.FileDataModel.new(java.io.File.new(filename))
end

def set_similatiry(type=$similarity_types[0])
	$similarity = case type 
	when "Pearson"
		MahoutSimilarity.PearsonCorrelationSimilarity.new($model)
	when "UncenteredCosine"
		MahoutSimilarity.UncenteredCosineSimilarity.new($model)
	when "Spearman"	
		MahoutSimilarity.SpearmanCorrelationSimilarity.new($model)
	when "LogLikelihood"
		MahoutSimilarity.LogLikelihoodSimilarity.new($model)
	when "Tanimoto" 
		MahoutSimilarity.TanimotoCoefficientSimilarity.new($model)
	else 
		MahoutSimilarity.EuclideanDistanceSimilarity.new($model)
	end
end

def set_neighborhood(neighborhood_size=$neighborhood_sizes[0])
	$neighborhood = MahoutNeighborhood.NearestNUserNeighborhood.new(neighborhood_size, $similarity, $model)
end

def set_recommender(type=$recommender_types[0])
	$recommender = case type 
	when "GenericBooleanPrefUserBased"
		MahoutRecommender.GenericBooleanPrefUserBasedRecommender.new($model, $neighborhood, $similarity)
	else
		MahoutRecommender.GenericUserBasedRecommender.new($model, $neighborhood, $similarity)
	end
end

def get_recommendation(user_id=$user_ids[0],recommendations_size=10)
	$recommender.recommend(user_id, recommendations_size)
end

def get_estimate(user_id,item_id)
	$recommender.estimatePreference(user_id,item_id)
end

def check_estimate(user_id=$user_ids[0],recommendations_size=10)
	for rec in get_recommendation(user_id,recommendations_size)
		item_id = rec.get_item_id
		etimates = $recommender.estimatePreference(user_id,item_id)
		etimates_b = (etimates > $threshold)?5:1
		puts "#{rec}: \t raw estimate: #{etimates} \t bool estimate: #{etimates_b}"
	end
end

def model_info(model)
	puts "Model Info............................"
	puts "Max Pref val: #{model.getMaxPreference()}"   
	puts "Min Pref val: #{model.getMinPreference()}"
           
	puts "Number of items: #{model.getNumItems()}"  	      
	puts "Number of users: #{model.getNumUsers()}"      
end

def open_csv_file(file_name)
    $csv_t  = CSV.open(file_name, 'w' ) 
end

def convert_to_bool(estimate)
	(estimate > $threshold)?5.0:1.0
end

def cal_error(true_val,estimate)
	estimate_b= convert_to_bool(estimate)
	error = true_val != estimate_b
	puts [true_val, estimate_b] if error  && !([1.0,5.0].include?(estimate_b))
	error 
end

def cal_recommendable(estimate, error)
	$recommendable_th.each_with_index{|th,ind|
		if estimate >= th 
			$recommendable_tc[ind] +=1
			$recommendable_ec[ind] +=1 if error
		end
	}
	ind = (convert_to_bool(estimate) < $threshold)?0:1
	$recommendable_type_tc[ind] +=1
	$recommendable_type_ec[ind] +=1 if error 


end
def log_recommendable(total_count)
	$recommendable_th.each_with_index{|th,ind|
		pcorrect = 0.0;
		pcorrect = 1.0-($recommendable_ec[ind].to_f/$recommendable_tc[ind]) if $recommendable_tc[ind]>0
		total_per = 0.0
		total_per = $recommendable_tc[ind].to_f/total_count if total_count > 0
		total_per = (total_per*100).to_i
		log_text "Recommendable:: Threshold #{th}\tTotal: #{$recommendable_tc[ind]}[#{total_per}%] \t Error Count: #{$recommendable_ec[ind]} \t %Correct: #{pcorrect}"
	}
	phate,plike,phate_e,plike_e =0,0,0,0
	phate = $recommendable_type_tc[0].to_f/total_count if total_count > 0
	plike = $recommendable_type_tc[1].to_f/total_count if total_count > 0
	phate_e = $recommendable_type_ec[0].to_f/$recommendable_type_tc[0] if $recommendable_type_tc[0] > 0
	plike_e = $recommendable_type_ec[1].to_f/$recommendable_type_tc[1] if $recommendable_type_tc[1] > 0

	log_text "Recommendation: %Like:#{plike}\t%Like Error:#{plike_e}\t%Hate:#{phate}\t%Hate Error:#{phate_e}"
end

def calculate_performance(fixed_count=0)
	total_count=0
	error_count = 0
	CSV.foreach($test_file_name) do |row|
		row=row.collect{|v| v.to_f}
		estimate = get_estimate(row[0],row[1])
		total_count +=1
		error = cal_error(row[2],estimate)
		error_count +=1 if error
		cal_recommendable(estimate,error)
		#puts [ row[2] , estimate, cal_error(row[2],estimate)]
		if total_count % 10000 == 0 
			pcorrect = 1.0-(error_count.to_f/total_count)
			log_text "Total: #{total_count} \t Error Count: #{error_count} \t %Correct: #{pcorrect}"
			log_recommendable(total_count)
		end
		break if total_count==fixed_count
	end
	pcorrect = 1.0-(error_count.to_f/total_count)
	log_text "Total: #{total_count} \t Error Count: #{error_count} \t %Correct: #{pcorrect}"
	log_text "--------------------------------------------"
	log_recommendable(total_count)
end

def log_text(text)
	puts text
	$logfile.puts text 
end

def run_all(test_size)
	for simtype in $similarity_types
		for nsize in $neighborhood_sizes
			for rcomtype in [$recommender_types[0]]
				run_test(simtype,nsize,rcomtype,test_size)
			end
		end
	end
end

def run_test(simtype,nsize,rcomtype,test_size)
	set_similatiry(simtype)
	set_neighborhood(nsize)	
	set_recommender(rcomtype)
	log_text "Data file: #{$data_file_name}\tSimilarity type: #{simtype}\tNeighborhood size: #{nsize}\t Recommender type: #{rcomtype}\t Test Size: #{test_size}"
	log_text "--------------------------------------------"
	calculate_performance(test_size)
end


def set_factorizer(factype,numFeatures,lambda,numIterations)
	$factorizer = case factype
	when "SVDPlusPlusFactorizer"
		MahoutRecommenderSVD.SVDPlusPlusFactorizer.new($model,numFeatures,numIterations)
	else
		MahoutRecommenderSVD.ALSWRFactorizer.new($model,numFeatures,lambda,numIterations)
	end

end

def set_persistency_filename(type,numFeatures,lambda,numIterations)
	tag = "_" + type.to_s
	tag += "_" + numFeatures.to_s
	tag += "_" + (lambda*100.0).to_i.to_s
	tag += "_" + numIterations.to_s
	$svd_data_file ="svd_data/"+$data_file_name.split(".")[0]+tag+ ".data"
end

def set_persistence(type,numFeatures,lambda,numIterations)
	set_persistency_filename(type,numFeatures,lambda,numIterations)
	pfile  = java.io.File.new($svd_data_file)
	$persistency_policy = MahoutRecommenderSVD.FilePersistenceStrategy.new(pfile)
end

def set_recommender_svd(type=$recommender_svd_types[0])
	$recommender = case type 
	when "SVDRecommender"
		if $persistency_policy
			MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer,$persistency_policy)
		else 
			MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer)
		end 
	else
		if $persistency_policy
			MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer,$persistency_policy)
		else
			MahoutRecommenderSVD.SVDRecommender.new($model,$factorizer)
		end 
	end
end

def run_sdv_all(factype,test_size)
	for numFeatures in $num_featues_vals
		for lambda in $lamda_vals
			for numIterations in $num_iterations_vals
				run_svd_test(factype,numFeatures,lambda,numIterations,test_size)
			end
		end
	end
end

def run_recommendation(numRec)
	numUsers=  $model.getNumUsers
	numUsersFrac = (numUsers/10.0).to_i 
	cUser=0;
	$model.getUserIDs.each{|uid|
		$recommender.recommend(uid,numRec)
		cUser +=1
		puts " % completed: #{(100.0 * cUser)/ numUsers}"  if  cUser%numUsersFrac==0
	}
end

def run_svd_test(factype,numFeatures,lambda,numIterations,test_size,numRec=nil)
	rcomtype = $recommender_svd_types[0]
	set_factorizer(factype,numFeatures,lambda,numIterations)
	set_persistence(factype,numFeatures,lambda,numIterations)
	set_recommender_svd($recommender_svd_types[0])
	log_text "Data file: #{$data_file_name}\tRecommender type: #{rcomtype}\tFactorization Type: #{factype} \t Num Features type: #{numFeatures}\tLambda: #{lambda}\tNum Of iteration: #{numIterations}\tTest Size: #{test_size}\tThreshold: #{$threshold}"
	log_text "--------------------------------------------"
	if numRec	
		run_recommendation(numRec)
	else
		calculate_performance(test_size)
	end
	
end

# set everything
set_model
model_info($model)


bench_mark = Benchmark.bm(7) do |item|
#item.report("neighborhood: ") {run_test($similarity_types[4],$neighborhood_sizes[2],$recommender_types[0],100000)}
#item.report("svd run 5: ") {run_svd_test($factorizer_types[0],50,0.2,20,0,5)}
#item.report("svd run 50: ") {run_svd_test($factorizer_types[0],50,0.2,20,0,50)}
#item.report("svd run 500: ") {run_svd_test($factorizer_types[0],50,0.2,20,0,500)}
#item.report("svd: ") {run_svd_test($factorizer_types[0],50,0.2,20,0)}
item.report("svd: ") {run_svd_test($factorizer_types[1],100,0.2,10,0)}
#item.report("svd: ") {run_sdv_all($factorizer_types[0],0)}
end

log_text bench_mark
log_text "--------------------------------------------"

