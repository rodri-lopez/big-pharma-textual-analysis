# The DATA

## Let's start with the data -- What are all these numbers?

### CSR_data/TDMs:

TDMs are **T**erm **D**ocument **M**atrices. **What does that mean?**

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
1. Pass in a corpus (a list of documents, in the form of very, *very* long strings) to the black box that is **CountVectorizer**
   1. Somehow it counts the number of matches against words in the vocabulary very very quickly
   2. Outputs the count to the matrix cell corresponding to the vocabulary word and the document
2. Returns the matrix

Not much more to say about TDMs. A fairly simple idea, but useful for our purposes: quantify the *CSR-ness* of companies in a *stable, repeatable, unbiased* way.

### CSR_data/TDMs_mapped_collapsed_normalized

What's all this about **mapped, collapsed, normalized?**

## Mapped
The [vocabulary](/CSR-Word-Taxonomy.txt) we used for this analysis is not just a list of words; it's a hierarchy. Why a hierarchy? Inspiration from [Baier, Berninger & Kiesel](https://onlinelibrary.wiley.com/doi/10.1111/fmii.12132). In their attempt to quantify the CSR-ness of a set of Fortune 500 companies, they established a hierarchical vocabulary with 3 main topics: **GOVERNANCE**, **SOCIAL**, and **ENVIRONMENTAL**.