# The DATA

### Let's start with the data -- What are all these numbers?

## CSR_data/TDMs:

#### TDMs are **T**erm **D**ocument **M**atrices. **What does that mean?**

TDMs show count values (counts of words). Think of a default TDM as if it were a histogram, but the 'buckets' are all the words in the corpus (the collection of documents). Those 'buckets' (words), correspond to the columns of a TDM matrix. The rows correspond to the individual documents within the corpus.

Let's take these 3 documents as an example
+ doc1: "The fox ran over the fence"
+ doc2: "The man built a big fence"
+ doc3: "Mr. Trump's big border fence"

**The resulting TDM:**
| document | the | fox | ran | over | fence | man | built | a | big | Mr. | Trump | border |
|----------|----------|----------|----------|----------|----------|----------|----------|----------|-----------|-----------|-----------|-----------|
| doc1 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| doc2 | 1 | 1 | 0 | 0 | 1 | 1 | 1 | 1 | 1 | 0 | 0 | 0 |
| doc3 | 1 | 1 | 0 | 0 | 1 | 0 | 0 | 0 | 1 | 1 | 1 | 1 |

Pretty simple idea. Here we display the counts for all the words in the corpus, from each document. In practice, we preprocess by removing stopwords and lemmatizing. After preprocessing, doc1 would be more like "fox ran over fence".

Furthermore, we don't care about counting every single word in the corpus. We care about a much smaller subset of the corpus, a *predetermined, CSR-specific* [vocabulary](/CSR-Word-Taxonomy.txt). Luckily, the python library sklearn.feature_extraction.text easily accomodates this need through its class **CountVectorizer**. For our purposes, we do not need to understand the inner workings of this class, as long as we understand the output. To get a raw output, the process is relatively simple: 
1. Pass in a vocabulary, in the form of a (relatively) short string, to **CountVectorizer** to create a vectorizer class instance
2. Pass in a corpus (a list of documents, in the form of very, *very* long strings) to the black box that is **CountVectorizer's** fit_transform() method
   1. Somehow it counts the number of matches against words in the vocabulary very very quickly
   2. Outputs the count to the matrix cell corresponding to the vocabulary word and the document
3. Returns the matrix

Not much more to say about TDMs. A fairly simple idea, but useful for our purposes: quantify the *CSR-ness* of companies in a *stable, repeatable, unbiased* way.

## CSR_data/TDMs_mapped_collapsed_normalized

#### What's all this about **mapped, collapsed, normalized?**

### Mapped
The [vocabulary](/CSR-Word-Taxonomy.txt) we used for this analysis is not just a list of words; it's a hierarchy. Why a hierarchy? Inspiration from [Baier, Berninger & Kiesel](https://onlinelibrary.wiley.com/doi/10.1111/fmii.12132). In their attempt to quantify the CSR-ness of a set of Fortune 500 companies, they established a hierarchical vocabulary with 3 main topics: **GOVERNANCE**, **SOCIAL**, and **ENVIRONMENTAL**, and a host of categories and subcategories within those topics. We've replicated this vocabulary, modifying in some cases but overall maintaining the same essence and hierarchical structure.

So, the **mapped** refers to mapping all the vocabulary terms in the [raw TDM](/TDMs/tdm_2000.csv) outputted by **CountVectorizer's** fit_transform() back to their corresponding *topic, category, and subcategory*. If you refer to this [mapped_collapsed_normalized TDM](/TDMs_mapped_collapsed_normalized/tdm_2000.csv), the bottom 3 rows show the mapping of a particular vocabulary word.

### Normalization 1
I decided to normalize the counts against the length of the document they were matched in. The thought being that a document 10,000 tokens long is likely to have more matches than one 5,000 tokens long, but may not necessarily deserve a better score. Therefore, the counts are divided by the number of vocabularies long a document is. The vocabulary is roughly 500 tokens long, so the counts in a document of 10,000 tokens are divided by ~20. 

### Collapsed
During the data collection stage, it became clear that, despite all being public firms, some companies publish more reports than others. In some cases, they may publish a normal annual report, as well as an ESG report, a finance report, an integrated report etc. Refer to the rows of this [raw TDM](TDMs/tdm_2017.csv) for an example. Now, the goal is to have a score per company, not per document. So, how to accomplish this? Some sort of an aggregation (collapse) needs to performed along the rows where companies have more than one report in the year. I thought a while about how to aggregate this. I wanted the aggregation to be fair. Using 'SUM' as the aggregating function, I felt this could skew the results significantly towards companies that simply just *put out more* in a given year. Naturally more words out, the more matches you will have. The normalization above would not have already accounted for this because these matches would be summed across different documents, of different lengths. At the same time, I felt that there should be some 'punishment' for not putting out *as much* as peers. GSK puts out an ESG report the last 5 years, and MRK never does - it seems obvious that MRK's CSR-ness should be punished relative to GSK. Ultimately, I've used 'MAX' as the aggregating function. The justification is that the more reports a company put out, the more chances they had at having a highest possible count score across the vocabulary.

### Normalization 2
Lastly, for every year and every word, I normalize/scale the count scores across every word using a **MinMaxScaler**. Every column Series **S** is passed to this scaler, which does the following for a value **a**:

$$
a_{\text{scaled}} = \frac{(a - \min(S))}{(\max(S) - \min(S))} \times (100 - 0) + 0
$$

Zeros remain zeros, but the top score $a = \max(S) = 100$. All other scores fall somewhere in the range: $[0, 100]$.

## CSR_data/SCORES

#### How did we get to these nice large numbers?

The [scores CSVs](SCORES/scores_2000.csv)  are the result of one final aggregation, which connect back to the vocabulary hierarchy mentioned in [mapped](#mapped). After **Normalization 2**, the count scores are ready to be aggregated into CSR topic scores. The program iterates through every **mapped_collapsed_normalized** TDM and computes a new DataFrame, which aggregates the scores **across columns (vocabulary words)** using 'SUM', leaving only 3 columns in the new DataFrame: **GOVERNANCE**, **SOCIAL**, and **ENVIRONMENTAL**, and rows for each ticker/firm.

## CSR_scores_timeseries.csv
The last step is to take the scores CSV for every year and compile them into one large timeseries. The gaps exist either because of a gap in data collection (rare) or, more usually, because the firm is 1) private, or 2) was private at some point during the observation period.


