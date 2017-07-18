# 2017-STAT-9794-PySparks

## Scratch area
The program is being developed and tested on Hortonwork's Sandbox VM image. More specifically, the platform is running on **Spark 2.1.0** with **Anaconda Python 3.5.3**. Below are important commands to save for setting up the enivronment to test on above specifications:

```bash
# This assumes you've installed Continuum's Python Anaconda distribution, setting
# it's primary python distribution to version 3.5.3
export PYSPARK_PYTHON=/root/anaconda3/bin/python
PYSPARK_DRIVER_PYTHON=/root/anaconda3/bin/ipython /usr/hdp/current/spark2-client/bin/pyspark
```
## Instruction for team
Quick instruction for the team on how to keep things ordered

### Directories
* `src` directory contains the main source files that runs the program
* `config` directory contains program's **json** configuration files
* `data` directory contains raw data used in this project. Data **should not** be saved in GIT, only locally on your machine
* `lib` directory contains other python files for class and function definition, emphasizing code reuse
* `log` directory contains raw execution and results log files

## Project breakdown
The project has mainly three parts:

### Data loading
The raw json files should be loaded into memory as compressed files, and only expanded within Python for reading. This helps reduce the bandwidth needed to load data from disk to memory. This is especially important for the slower hard disk you may or may not have

### Textual processing
The focus of this part is to extract the raw text and process it

- **Filter for english tweets**

 Their should be a field (think its called `lang`) that specifies the language. For english, this is `lang = "en"`.. This is not perfect however. I've seen tweets classified as english where the text is clearly not. No big deal, we will just continue to use it

- **Clean text and feature extraction**

 We will try the simple method first, which is counting the number of times the company is mentioned in a tweet. For each tweet, we create a new field that contains that number

 Hence, we will have a simple time series data with datetime and count of such mentions, for every company we're interested in. If there is no mention, we set it to zero of course

 Ensure this is a function. We will try a more advanced method, such as a Naive Bayes Classifier. Such a method will require more features then simply count. If so, below will list out the necessary steps

 * Lowercase the text
 * Remove @mentions and #hashtags, but keep the ones that mentions/hashtags companies
 * Remove hyperlinks, unless it's the company-related website
 * When removing above, replace them with whitespace
 * Trim whitespace (remove leading and trailing whitespaces) and replace sequences of whitespace of or more to just one whitespace
 * Tokenize the text, which is splitting the text into the individual words with whitespace. For example, the term `"data analysis"` becomes a list containing `'data'` and `'analysis'` e.g. `['data', 'analysis']`
 * Remove stopwords, such as *the* and *and*, to remove noise

 Lets stick with the simple counts for now

### Train a simple linear regression

 The dependent variable will be the company's closing price, while the independent variable are the count of mentions

 We will need to split the data in training and testing. The training set is used to train the regression model, while the test is to test perform out-of-sample. We could use a simple time-based split, like say if we only had a year of data, the first 8 months is used for training and the last 4 months used for testing

### How to program

Lets try to stick with creating functions. For example, a function that takes a text string and cleans it. Another function that then takes that cleaned text and generates the features, etc.

This allows us to make edits to one function when we want to change it, then the changes will propagate throughout the program
