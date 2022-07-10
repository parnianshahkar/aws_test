# aws_test


athena_lookup.py: 
Purpose: This file takes a list of urls as its input and crawls the urls for the selected timeframe from common crawl. Then it outputs the subpages that contain specific keywords and the n shortest urls of subpages, where the specific keywords and n are determined by user. 
Input example: no www. before. 
Code explanation: The main function that is executed in the Athena_lookup class is the run_lookup(self) function. This function calls some other important functions. The description for the tasks of each function is as following: 
drop_all_tables(self): This function drops tables if there exists any. 
create_url_list_table(self): This function creates a new table with columns "websiteaddress" and "bvdidnumber".
create_ccindex_table(self): This function creates a large table by selecting some columns from the original common crawl database.
repair_ccindex_table(self): ?
inner_join(self): This function merges the two tables created through create_ccindex_table(self) and create_url_list_table(self) functions. The resulting table contains the name and address of all the historical subpages of the urls in the url list.   
select_subpages(self): The merged table created through the inner_join(self) function is very large. Therefore, we only select the subpages that contain a specific keyword in their website address, and the n_subpages shortest urls. n_subpages is one of the parameters that is determined by the user. 





