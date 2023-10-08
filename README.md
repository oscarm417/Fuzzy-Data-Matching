# Fuzzy-Data-Matching
This package helps identify and remove duplicate records from large data sets.
Additionally, you can adjust the similarity of thresholds, to identify approximate matches.
This was created for identifying duplicate submissions for class action lawsuits.  Hope this helps you clean your data sets, identify duplicate records, or standardize data.

```
Example 1: 

Assume file name is called "hireMe.xlsx" and "peopleHired.xlsx" exist. We want to identify who got hired, but data entry may not be perfect

hireMe = pd.read_excel("hireMe.xlsx", dtype =str).fillna("")
hired = pd.read_excel("peopleHired.xslx", dtype =str).fillna("")
duplicateRecordsMatched = fuzzy_compare_dataframes(hireMe, hired, deduping_cols1 = ['Name','Email','Address'],deduping_cols2 = ['Name','Email','Address'], fuzzy_percentage = .95)

#This will give you the duplicate records, based on the 95% match. 


Example 2: 
Lets say we want to identify people that submited multiple submissions to a settlement that only allows one submission per person. 
In many instances, people trying to submit many times, will submit many times with very small changes, as so to be considered different people, 
but still be able to cash the checks.

df = pd.read_excel("submissions.xlsx", dtype =str).fillna("")
duplicateRecordsMarked = fuzzy_dedupe_main(df,deduping_cols1 = ['FirstName','LastName'],percent_match = .9)

At this point all duplicate records will be matched with a unique id, and a rank value. The rank value identies the order in which that duplicate record was found. 
So the first duplicate record will be marked as Rank = 1

Duplicate records can then be removed with the following command

cleanedDf = duplicateRecordsMarked[duplicateRecordsMarked['Rank']==1]

```
