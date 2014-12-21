--eachCat + DELIMITER + xP.getHash() + DELIMITER + xP.getUrl() + DELIMITER + postive + DELIMITER + negative + DELIMITER + xP.getUsercount();

links = load '/user/training/linkData/clean/' using PigStorage(',') as (category:chararray ,hash:chararray, url:chararray , positive:int , negative:int ,totalreview:int );



url_cat = foreach links generate hash ,TRIM(category); 
store url_cat into '/user/training/linkData/category';

url_review =  foreach links generate  hash  , url , positive, negative, totalreview; 
url_review_distinct = distinct url_review;
store url_review_distinct into '/user/training/linkData/review';

