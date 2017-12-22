# Document Search Engine

A search engine to find the most relevant document in response to a search string (query) by computing scores using term frequency-inverse document frequency (TF-IDF).

## Execution Instructions:
### Environment: Cloudera
Create input folder and move all the input files into it

### 1. DocWordCount:
Give input and output paths as arguments.
```
hadoop jar <jarFileName> DocWordCount <input> <output>
```
### 2. TermFrequency
Give input and output paths as arguments
```
hadoop jar <jarFileName> DocWordCount <input> <output>
```
### 3. TFIDF job
Give input and output paths as arguments
```
hadoop jar <jarFileName> TFIDF <input> <output>
Note: Since we use chaining of TFIDF with TermFrequency an output file with the name “intermediate”. So, please delete this folder every time you run the jar.
```
### 4. Search
Run the TFIDF job before running the Search job.
Provide the output folder of TFIDF as input, output path and a search query as arguments.
```
hadoop jar <jarFileName> Search <tfidf_output_patht> <output> <search_query>
```
### 5. Rank
Run the Search algorithm before Rank.
```
hadoop jar <jarFileName> Rank <Search_ouput_path> <output>
```
