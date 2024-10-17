# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector
from mysql.connector import Error 
from scrapy.exceptions import DropItem
import logging
from datetime import datetime
from dotenv import load_dotenv
import os
from supabase import create_client, Client
from scrapy.exceptions import NotConfigured

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
import re
import spacy
import mysql.connector  # Needed for MySQL database connection

import openai

class AccumulatePipeline:
    """
    A pipeline that accumulates all items processed by spiders.
    
    This pipeline is designed to collect all items scraped during the run
    of multiple spiders or multiple runs of the same spider, allowing for
    bulk processing or analysis at a later stage.
    """
    # This class variable will store all items from all spiders
    accumulated_items = []

    def process_item(self, item, spider):
        # Add the item to the class-level list
        try:
            # Add the item to the class-level list
            self.__class__.accumulated_items.append(item)
        except Exception as e:
            spider.logger.error(f"Error accumulating item: {e}")
        # Make sure to return the item to continue the pipeline process
        return item

    @classmethod
    def get_accumulated_items(cls):
        """
        Provides access to all accumulated items.

        Returns:
            A list of all items accumulated over the course of the spider(s) run.
        """
        # Access method to get all accumulated items
        return cls.accumulated_items

class CrawlerPipeline:
    # pass
    def __init__(self):
        """
        Initializes the spider with necessary configurations and resources.
        
        - Loads environment variables.
        - Initializes the SentenceTransformer model.
        - Validates and sets up Supabase connection.
        - Fetches existing headers embeddings from the database.
        """
        load_dotenv()
        try:
            # Initialize SentenceTransformer model
            self.model = SentenceTransformer('multi-qa-mpnet-base-cos-v1')
        except Exception as e:
            raise NotConfigured(f"Error initializing SentenceTransformer model: {e}")
        
        
        
        # Retrieve Supabase connection parameters from environment variables
        supabase_url = os.environ.get('SUPABASE_URL')
        supabase_key = os.environ.get('SUPABASE_KEY')
        if not supabase_url or not supabase_key:
            raise NotConfigured('Supabase URL and Key must be set as environment variables')

        try:
            print('Initializing Supabase client...')
            # Create a Supabase client instance
            self.supabase: Client = create_client(supabase_url, supabase_key)
        except Exception as e:
            raise NotConfigured(f"Error initializing Supabase client: {e}")

        try:
            # Fetch existing headers embeddings from the database
            self.existing_headers_embeddings = self.fetch_existing_headers_embeddings()
        except Exception as e:
            # Log and handle any errors encountered during fetch operation
            raise NotConfigured(f"Error fetching existing headers embeddings: {e}")
        
        self.item_cache = [] 

    def fetch_existing_headers_embeddings(self):
        """
        Fetches existing news headers from the Supabase database and generates embeddings.

        Uses the SentenceTransformer model initialized in the spider to encode the headers into embeddings.

        Returns:
            A tensor of embeddings if there are existing headers, otherwise an empty numpy array.
        """
        # Fetch existing headers from the database and create embeddings
        result = self.supabase.table('news').select('header').execute()
        try:
            existing_headers = [row['header'] for row in result.data] if result.data else []
            # Check if there are any headers to encode
            if existing_headers:
                embeddings = self.model.encode(existing_headers, convert_to_tensor=True)
            else:
                # Handle the case where there are no existing headers
                embeddings = np.array([])  # Create an empty array or handle appropriately
        except Exception as e:
            # Log and handle any errors encountered during the encoding process
            logging.error(f"Error encoding existing headers: {e}")
            embeddings = np.array([])

        return embeddings

    def header_similarity(self, header):
        """
        Calculates the cosine similarity between a new header's embedding and existing headers' embeddings.

        Args:
            header (str): The text of the new header for which similarity is to be calculated.

        Returns:
            float: The highest cosine similarity score with the existing headers. Returns 0 if there are no existing headers to compare.
        """

        try:
            # Create an embedding for the new header and compare with existing ones
            new_header_embedding = self.model.encode(header, convert_to_tensor=True)
            if self.existing_headers_embeddings.size == 0:
            # Return a default similarity value (e.g., 0) if there are no existing embeddings
                return 0
            else:
                # Calculate similarity only if there are existing embeddings
                similarities = cosine_similarity(
                    new_header_embedding.reshape(1, -1),
                    self.existing_headers_embeddings
                )
                return np.max(similarities)
        except Exception as e:
            # Log any errors encountered during the process.
            logging.error(f"Error calculating header similarity for '{header}': {e}")
            # Return a default value to indicate failure in calculation.
            return -1
    
    def process_item(self, items):
        """
        Processes each item by calculating its header similarity and inserting it into Supabase if unique enough.
        
        Args:
            item: The item being processed.
            spider: The spider that scraped the item.
            
        Returns:
            The item if it was successfully processed and inserted into the database.
        """
        table = "news"
        for item in items:
            try:
                similarity = self.header_similarity(item.get('header'))
                logging.info(f"Similarity for {item.get('header')}: {similarity}")
            except Exception as e:
                logging.error(f"Error calculating similarity: {str(e)}")
                raise DropItem("Error calculating similarity.")
            
            # If the item does not exist, proceed with insertion
            data = {
                'created_at': item.get('date'),
                'label': item.get('label'),
                'header': item.get('header'),
                'sub_header': item.get('sub_header'),
                'img': item.get('img'),
                'img_caption': item.get('img_caption'),
                'content': item.get('content')
            }
            if similarity < 0.98:
                try:
                    response = self.supabase.table(table).insert(data).execute()
                    logging.info("Item inserted to Supabase successfully")
                    self.item_cache.append(item)  # Add the item to the cache
                except Exception as e:
                    # Handle any exceptions thrown during the insert attempt, which may include HTTP errors
                    logging.error(f"Error inserting item to Supabase: {str(e)}")
                    raise DropItem(f"Error inserting item to Supabase: {str(e)}")
            else:
                logging.info(f"Item not inserted due to high similarity: {item.get('header', '')}")
                raise DropItem(f"Item too similar to existing entries: {item}")

        return self.item_cache

class ComparePipeline:
    """
    A class responsible for comparing and processing articles, including NLP tasks and similarity calculations
    among articles to group similar ones based on their content.
    """
    # pass
    def __init__(self):
        """
        Initializes the ComparePipeline with pre-loaded NLP models and sets the similarity threshold.
        """
        try:
            self.nlp = spacy.load('en_core_web_md')
        except Exception as e:
            # Handle exceptions related to SpaCy model loading
            print(f"Failed to load SpaCy model: {e}")
            
        try:
            self.model = SentenceTransformer('multi-qa-mpnet-base-cos-v1')
        except Exception as e:
            # Handle exceptions related to Sentence Transformer model loading
            print(f"Failed to load Sentence Transformer model: {e}")
        
        self.threshold = 0.85
        self.grouped_articles = []

    def preprocess_text(self, text):
        """
        Preprocesses the given text to prepare it for further NLP tasks.

        The preprocessing includes converting the text to lowercase, removing stopwords,
        punctuation, and numbers, and then lemmatizing the tokens.

        Args:
            text (str): The text to be preprocessed.

        Returns:
            str: The preprocessed and lemmatized text as a single string.
        """
        doc = self.nlp(text.lower())
        lemmatized_text = ' '.join([token.lemma_ for token in doc if not token.is_stop and not token.is_punct and not token.like_num])
        return lemmatized_text

    def compare_news_articles(self, contents):
        """
        Calculates the cosine similarity between embeddings of a set of news article contents.

        This method uses a pre-trained SentenceTransformer model to generate embeddings
        for each article's content, then computes the cosine similarity between every pair
        of articles to identify their level of similarity.

        Args:
            contents (List[str]): A list containing the text content of each news article.

        Returns:
            np.ndarray: A 2D numpy array representing the cosine similarity matrix between all pairs of articles.
        """
        try:
            embeddings = self.model.encode(contents)
            cosine_sim = cosine_similarity(embeddings, embeddings)
            return cosine_sim
        except Exception as e:
            # Log the exception if the embedding generation or similarity calculation fails
            logging.error(f"Failed to compare news articles: {e}")
            return np.empty((0, 0))

    def find_unique_similar_article_pairs(self, similarity_matrix, threshold):
        """
        Identifies pairs of articles that are similar to each other above a specified threshold.

        Args:
            similarity_matrix (np.ndarray): A 2D numpy array containing similarity scores between articles.
            threshold (float): The minimum similarity score to consider two articles as similar.

        Returns:
            dict: A dictionary where keys are article indices and values are lists of tuples, 
                each tuple containing the index of a similar article and their similarity score.
        """
        grouped_articles = set()  # Keep track of articles already in a group
        article_similarities = {}
        for i in range(len(similarity_matrix)):
            if i not in grouped_articles:
                similar_articles = [(j, similarity_matrix[i, j]) for j in range(len(similarity_matrix)) if similarity_matrix[i, j] > threshold and i != j and j not in grouped_articles]
                article_similarities[i] = similar_articles
                grouped_articles.add(i)
                grouped_articles.update([j for j, _ in similar_articles])
        return article_similarities

    def process_grouped_articles(self, items):
        """
        Processes items to find and group similar articles based on their content similarity.
        
        Args:
            items (list of dict): A list of article items, where each item is expected to have
                                at least 'content' and 'header' fields.
        
        Returns:
            dict: A dictionary where each key is the index of an article in `items` and the value
                is a list of articles that are similar to it, including the article itself.
        """

        if items:
            contents = [self.preprocess_text(item['content'] if item['content'] is not None else item['header']) for item in items]
            similarity_matrix = self.compare_news_articles(contents)
            article_similarities = self.find_unique_similar_article_pairs(similarity_matrix, self.threshold)

            grouped_articles_full = {}

            # closely monitor the output
            with open('similar_articles.txt', 'w', encoding='utf-8') as file:
                for i, similar_indices in article_similarities.items():
                    # Initialize the group with the base article
                    grouped_articles_full[i] = [items[i]]  # Start the group with a list containing the base article
                    for index, _ in similar_indices:
                        grouped_articles_full[i].append(items[index])  # Append similar articles to the group

                    file.write(f"Group starting with Article {i + 1}:\n")
                    base_article_content = items[i]['content']
                    file.write("Base Article Content:\n")
                    file.write(base_article_content + "\n\n")
                    
                    if similar_indices:
                        file.write("Similar Articles:\n")
                        for index, similarity in similar_indices:
                            similar_article_content = items[index]['content']
                            file.write(f"\tArticle {index + 1} (Similarity: {similarity:.2f})\n")
                            file.write("\tContent:\n")
                            file.write(similar_article_content + "\n\n")
                    else:
                        file.write("\tNo similar articles above the threshold.\n\n")
                    
                    file.write("="*80 + "\n\n")
            
            self.grouped_articles = grouped_articles_full

            return self.grouped_articles


class DraftPipeline: 
    # pass
    def __init__(self):
        """
        Initializes the class instance by setting up OpenAI API access and establishing
        a database connection using credentials stored in environment variables.
        
        Raises:
            ValueError: If the OpenAI API key is not found in the environment variables.
            mysql.connector.Error: If connecting to the MySQL database fails.
        """
        load_dotenv()
        api_key =  os.environ.get('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OpenAI API key not found in environment variables.")
        self.api_key = api_key
        openai.api_key = self.api_key

        try:
            # Retrieve database credentials from environment variables for security
            db_host = os.environ.get('DB_HOST')
            db_name = os.environ.get('DB_NAME')
            db_user = os.environ.get('DB_USER')
            db_password = os.environ.get('DB_PASSWORD')

            # Establish a connection to the database
            self.conn = mysql.connector.connect(
                host=db_host,
                database=db_name,
                user=db_user,
                password=db_password
            )
            # Create a cursor object using the connection
            self.cur = self.conn.cursor()
        except Error as e:
            # Log or print the error if the database connection fails
            print(f"Failed to connect to database: {e}")
            raise 
        # # db here
        # self.conn = mysql.connector.connect(
        #     host='20.24.22.27',
        #     database='techtodate_test1',
        #     user='techtodateuser@localhost',
        #     password='techTodate'
        # )
        # self.cur = self.conn.cursor()
    

    def aggregate_articles_info(self, articles):
        """Combine contents from a list of articles into a single string."""
        aggregated_info = ""
        for article in articles:
            content = article.get('content', 'No Content')
            aggregated_info += f"{content}\n\n"
        return aggregated_info

    def draft_article_with_gpt(self, aggregated_content):
        
        """Use OpenAI's GPT to draft an article based on aggregated content."""
        try:
            prompt = (
                "As a professional writer skilled in web content creation, craft a compelling, structured, "
                "and visually appealing article using HTML. Start with an engaging header encapsulated within an <h1> tag, "
                "followed by insightful subheaders within <h2> tags to organize the content, enhancing readability and flow. "
                "Each section of the content should be wrapped in <p> tags. Apply inline CSS styles directly within these tags "
                "to enhance the visual appeal, focusing on readability and professional aesthetics. Ensure the article is coherent, "
                "well-structured, and tailored for an informed audience. The final output should be ready for web publication.\n\n"
                "Information to Include:\n"
                f"{aggregated_content}\n\n"
                "Please format your response with HTML tags and inline CSS, aiming for a polished and engaging presentation. "
                "Example: <h1 style='color: #333; font-family: Arial, sans-serif;'>Your Header Here</h1>"
            )
            
            chat_completion = openai.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
            )

            # Extract the HTML/CSS formatted content
            response_text = chat_completion.choices[0].message.content
            
            # Parse the response to extract header, subheader, and content
            header_start = response_text.find('<h1')
            header_end = response_text.find('</h1>') + 5
            subheader_start = response_text.find('<h2', header_end)
            subheader_end = response_text.find('</h2>') + 5 if subheader_start != -1 else header_end
            content_start = subheader_end if subheader_end != -1 else header_end
            
            drafted_header = response_text[header_start:header_end] if header_start != -1 else '<h1 style="color: black; font-size: 24px; font-family: Arial, sans-serif;">Draft Header</h1>'
            drafted_subheader = response_text[subheader_start:subheader_end] if subheader_start != -1 and subheader_end != -1 else ""
            drafted_content = response_text[content_start:].strip() if content_start != -1 else '<p style="color: #333; font-size: 16px; line-height: 1.6; font-family: Arial, sans-serif;">Draft Content</p>'
            

            # Add fixed styling if it was not included by GPT
            drafted_header = re.sub('<[^>]+>', '', drafted_header)
            drafted_subheader = drafted_subheader.replace('<h2>', '<h2 style="color: black; font-size: 18px; font-family: Arial, sans-serif;">')
            drafted_content = drafted_content.replace('<p>', '<p style="color: #333; font-size: 16px; line-height: 1.6; font-family: Arial, sans-serif;">')

            return drafted_header, drafted_subheader, drafted_content
        except Exception as e:
            logging.error(f'Error drafting article with GPT: {e}')
            return None, None, None
        
    def close(self, items):

        """
        Finalizes the processing of grouped articles by drafting content and inserting it into a database.

        Args:
            items (dict): A dictionary of grouped articles, where each key is a group ID and
                        each value is a list of articles in that group.
        """
        # Ensure that 'items' is a dictionary before proceeding
        if not isinstance(items, dict):
            logging.error("Expected 'items' to be a dictionary.")
            return
        
        grouped_articles = items

        if not grouped_articles:
            logging.info("No grouped articles to process.")
            return
        
        # closely monitor the output
        with open('drafted_articles.txt', 'w', encoding='utf-8') as file:
            for group_id, articles in grouped_articles.items():
                aggregated_content = self.aggregate_articles_info(articles)
                
                drafted_header, drafted_subheader, drafted_content = self.draft_article_with_gpt(aggregated_content)
                
                if drafted_content:
                    file.write(f"Group {group_id} Draft:\nHeader: {drafted_header}\nSubheader: {drafted_subheader}\nContent:\n{drafted_content}\n")
                    file.write("="*80 + "\n\n")  # Separator for readability
                    self.insert_into_db(drafted_header, drafted_subheader, drafted_content)
                    
                    logging.info(f"Draft for Group {group_id} saved.\n")
                else:
                    logging.error(f"Failed to draft content for Group {group_id}")


    def insert_into_db(self, header, subheader, content):
        post_type = 'post'
        # prepare post mariaDB !!!!!!!!!!
        ########################################################################################################################################################

        # Default values for WordPress fields
        default_author_id = 1  # Example: ID of the admin or a default user
        default_post_status = 'publish'  # or 'publish' if the posts are ready to go live
        # default_post_type = 'financial'  # assuming these are standard posts
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  

        ########################################################################################################################################################

        # Prepare data tuple with mapped and default values
        data_tuple = (
            default_author_id,
            current_datetime,
            current_datetime,  # post_date_gmt
            content,
            header,
            subheader,  # post_excerpt I turned it into a sub_header container which should work?
            default_post_status,
            'open',  # comment_status
            'open',  # ping_status
            '',  # post_password slugify(item.get('sub_header', ''))
            '',  # post_name (slug), use a slugify function
            '',  # to_ping
            '',  # pinged
            current_datetime,  # post_modified
            current_datetime,  # post_modified_gmt
            '',  # post_content_filtered
            0,   # post_parent
            '',  # guid
            0,   # menu_order
            post_type, # post_type
            'Central Asia',  # post_mime_type
            0,   # comment_count
        )


        # insert query here
        insert_query = """INSERT INTO wp_posts (
            post_author, post_date, post_date_gmt, post_content, post_title, post_excerpt, 
            post_status, comment_status, ping_status, post_password, post_name, to_ping, 
            pinged, post_modified, post_modified_gmt, post_content_filtered, post_parent, 
            guid, menu_order, post_type, post_mime_type, comment_count) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        ##########################################################################################################################################################

        # data_tuple = (item['date'],item['label'],item['header'],item['sub_header'],item['img'],item['img_caption'],item['content'])
        try:
            # Check if "Financial" category exists, if not, create it
            financial_category_id = self.get_category_id("Central Asia")
            if financial_category_id is None:
                term_id = self.create_category("Central Asia")
                financial_category_id = self.create_term_taxonomy(term_id)

            # check_query = "SELECT * FROM sim76_posts WHERE post_title = %s"
            # self.cur.execute(check_query, header)
            # if self.cur.fetchone() is None:

            # date, label, header, sub_header, img, img_caption, content) VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            self.cur.execute(insert_query, data_tuple)

            new_post_id = self.cur.lastrowid  # Get the ID of the new post

            # Link post to the "Financial" category
            insert_relationship_query = "INSERT INTO wp_term_relationships (object_id, term_taxonomy_id) VALUES (%s, %s)"
            self.cur.execute(insert_relationship_query, (new_post_id, financial_category_id))


            self.conn.commit()
            logging.info(f'Inserted new {post_type} with ID {new_post_id}')
                        
            # else:
            #     logging.info("Record already exists. Skipping insertion.")
        except Exception as e:
            logging.error(header)
            logging.error(subheader)
            logging.error(content)
            logging.error(f'Failed to connect to DB: {e}')

    def create_category(self, category_name):
        """
        Inserts a new category into the database.

        Args:
            category_name (str): The name of the category to be created.

        Returns:
            int: The ID of the newly created category term, or None if insertion fails.
        """
        insert_term_query = "INSERT INTO wp_terms (name, slug) VALUES (%s, %s)"
        slug = category_name.lower().replace(" ", "-")  # Simple slug creation
        self.cur.execute(insert_term_query, (category_name, slug))
        return self.cur.lastrowid  # Returns the newly created term_id
    
    def create_term_taxonomy(self, term_id, taxonomy='category', description=''):
        """
        Inserts a new term taxonomy into the database.

        Args:
            term_id (int): The term ID to associate the taxonomy with.
            taxonomy (str, optional): The type of taxonomy. Defaults to 'category'.
            description (str, optional): The taxonomy description. Defaults to an empty string.

        Returns:
            int: The ID of the newly created term taxonomy, or None if insertion fails.
        """
        insert_taxonomy_query = "INSERT INTO wp_term_taxonomy (term_id, taxonomy, description) VALUES (%s, %s, %s)"
        self.cur.execute(insert_taxonomy_query, (term_id, taxonomy, description))
        return self.cur.lastrowid  # Returns the newly created term_taxonomy_id
    
    def get_category_id(self, category_name):
        """
        Retrieves the category ID for a given category name.

        Args:
            category_name (str): The name of the category.

        Returns:
            int or None: The category ID if found, or None if the category does not exist.
        """
        query = """
        SELECT tt.term_taxonomy_id
        FROM wp_terms AS t
        INNER JOIN wp_term_taxonomy AS tt ON t.term_id = tt.term_id
        WHERE t.name = %s AND tt.taxonomy = 'category'
        """
        self.cur.execute(query, (category_name,))
        result = self.cur.fetchone()
        return result[0] if result else None









