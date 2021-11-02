# combine-csv


Assumtption:  
	* The following directory structure exists:
			combine-csv
				|
				|---dag
				|
				|---data
       
	
  * The code to be executed resides in a subdirectory 'dag' and the data files to be processed, as well as results reside in 'data' subdirectory.  Both subdirecotries at the same level. 
    
Instructions:
  * This code was developed using Python 3.9.6.  The recomendation is to run the code using Python 3.5 and above.
  * Prior to running the code, the following steps must be completed:
      1.- Ensure the directory structure descibed in the "Assumptions" section above exists.
      2.- Create a new python environment with python version 3.5 or above
      3.- Once having the python environment activated, run the following command `pip install -r requirements.txt`
      4.- Place any .csv files you desire to process in the `combine-csv/data` directory.
          > The name for the files to be processed must follow the following standard:  "<Description or region> <Environment> <Optional numeric value>.csv"         
            otherwise, the process will skip the file.
  * To run the code, you must use the following command:  `python3 <pathToRepo>/dag/combine_csv.py`
  * To run the unit tests prepared for this code, you must run the following command:  `pytest <pathToRepo>/dag/test_combine_csv.py`
  
