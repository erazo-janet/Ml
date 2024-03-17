import logging
import sys
import lucene
import os
import json
from org.apache.lucene.store import SimpleFSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import WhitespaceAnalyzer
from org.apache.lucene.analysis.core import KeywordAnalyzer
from org.apache.lucene.index import Term
from org.apache.lucene.search import BooleanQuery, BooleanClause, TermQuery, Query
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.queryparser.classic import MultiFieldQueryParser
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, IndexOptions, DirectoryReader
from org.apache.lucene.search import IndexSearcher, ScoreDoc
from org.apache.lucene.search.similarities import BM25Similarity

lucene.initVM()
logging.disable(sys.maxsize)

#read json files
def read_json_files(directory_path):
    json_data_list = []

    if not os.path.isdir(directory_path):
        print(f"{directory_path} is not a valid directory.")
        return json_data_list

    try:
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)

            if filename.endswith('.json'):
                print(f"Reading data from {filename}:")
                try:
                    with open(file_path, 'r') as file:
                        for line in file:
                            try:
                                json_data = json.loads(line.strip())
                                json_data_list.append(json_data)
                            except json.JSONDecodeError as e:
                                print(f"Error decoding JSON in {filename}: {e}")
                except IOError as e:
                    print(f"Error reading file {filename}: {e}")
    except Exception as e:
        print(f"Error occurred: {e}")

    return json_data_list


def create_index(dir, json_data_list):
    if not os.path.exists(dir):
        os.mkdir(dir)
    store = SimpleFSDirectory(Paths.get(dir))

    #define analyzers
    analyzer = StandardAnalyzer()
    whitespace_analyzer = WhitespaceAnalyzer() #helps split text based on whitespaces for tokenizing
    keyword_analyzer= KeywordAnalyzer() #for fields that do not need to be tokenized such as id
    config = IndexWriterConfig(analyzer)
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)  #this creates the index
    writer = IndexWriter(store, config) #this places the created index file and places it into the file directory specified in the code

    #This is the part of the field types
    subreddit_field = FieldType() #lets us filter or search by subreddit
    subreddit_field.setStored(True) #storing the value
    subreddit_field.setTokenized(True) #breaking down text to words for searching
    subreddit_field.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) #the index stores the term freq and position for searching

    title_field = FieldType() #searching with post titles
    title_field.setStored(True)
    title_field.setTokenized(True)
    title_field.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    self_text_field = FieldType() #searching text based posts
    self_text_field.setStored(True)
    self_text_field.setTokenized(True)
    self_text_field.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    comments_field = FieldType() #serching comments from posts
    comments_field.setStored(True)
    comments_field.setTokenized(True)
    comments_field.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

    id_field = FieldType() #unique identifier of a post
    id_field.setStored(True)
    id_field.setTokenized(False)#we do not need id to be tokenized because breaking down id to values would not be meaningful for searching

    reddit_url = FieldType()
    reddit_url.setStored(True)
    reddit_url.setTokenized(False) #dont need to tokenize for searching
    reddit_url.setIndexOptions(IndexOptions.DOCS)

    image_field = FieldType()
    image_field.setStored(True)
    image_field.setTokenized(False) #dont need to tokenize for searching since this is given to us as a unique identifier
    image_field.setIndexOptions(IndexOptions.DOCS)

    username = FieldType()
    username.setStored(True)
    username.setTokenized(False)
    username.setIndexOptions(IndexOptions.DOCS) #since we are not tokenizing, we dont need to store term frequency and position

    upvotes = FieldType()
    upvotes.setStored(True)
    upvotes.setTokenized(False) #upvotes are a numerical value, and does not need to be broken fown for searching purposes
    upvotes.setIndexOptions(IndexOptions.DOCS)#since we are not tokenizing, we dont need to store term frequency and position

    downvotes = FieldType()
    downvotes.setStored(True)
    downvotes.setTokenized(False) #upvotes are a numerical value, and does not need to be broken fown for searching purposes
    downvotes.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)


    #this code should be used to read the json files - needs to be modified
    for data in json_data_list:
        title = data['Title'] #extracting title from json data
        self_text = data.get('Self text','') #extracting the body of a reddit post from the json data
        subreddit_name = data.get("Subreddit","Unknown") #extracting the subreddit name from the json data

        doc = Document() #creating lucene document

        #adding the fields to the document
        doc.add(Field('Subreddit', subreddit_name, subreddit_field))  # Add subreddit field to document
        doc.add(Field('Title', title, title_field))  # Adding title field to document
        doc.add(Field('Self text', self_text, self_text_field))  # Adding self text field to document
        writer.addDocument(doc) #adding the document to the index

    writer.close() #closing the index after adding all documents


#creating our retrieval
def retrieve_documents(index_dir, query_text, top_k=5):
    directory = SimpleFSDirectory(Paths.get(index_dir)) #lucene directory where the index is stored
    searcher = IndexSearcher(DirectoryReader.open(directory)) #search the index

    analyzer = StandardAnalyzer() #lucene analyzer to break down text into terms through the indexing process

    fields = ["Subreddit", "Title", "Self text"] #the fields we decided the user can search from

    # Create a list to store parsed queries for each field
    field_queries = []
    for field in fields:
        field_query_parser = QueryParser(field, analyzer)
        field_query = field_query_parser.parse(QueryParser.escape(query_text))
        field_queries.append(field_query)

    # Create a BooleanQuery to combine all field queries
    boolean_query = BooleanQuery.Builder()
    for field_query in field_queries:
        boolean_query.add(field_query, BooleanClause.Occur.SHOULD)
    final_query = boolean_query.build()

    top_docs = searcher.search(final_query, top_k).scoreDocs #searches for documents that match the parsed query and gets the top documents based on the score
    top_k_docs = []

    #for loop to retrieve the docs from the index, extracts the values and creates a dictionary with the document and its score
    for hit in top_docs:
        doc = searcher.doc(hit.doc)
        top_k_docs.append({
            "Title": doc.get("Title"),
            "Subreddit": doc.get("Subreddit"),
            "Self text": doc.get("Self text"),
            "Score": hit.score #score
        })

    return top_k_docs

if __name__ == "__main__":
    json_files_dir = '/home/cs242/sample_data' #path where our json files are found
    json_data_list = read_json_files(json_files_dir) #reads json files and adds it to a list
    create_index('sample_lucene_index/', json_data_list) #where the index will be located
    print("Indexing completed successfully.")

    query_text = input("Enter your search query: ") #prompts the user to input a search
    top_k_docs = retrieve_documents('sample_lucene_index/', query_text) #look through the index for documents matching the users query

    print("Search results:") #print information of documents found from the users search results
    for idx, result in enumerate(top_k_docs, start=1):
        print(f"Document {idx}:")
        print(f"Title: {result['Title']}")
        print(f"Subreddit: {result['Subreddit']}")
        print(f"Self text: {result['Self text']}")
        print(f"Score: {result['Score']}")
        print()

