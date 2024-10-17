# Spider

# To use the requirement run the following codes:
python -m venv .venv

.venv\Scripts\activate  

# On Windows

source .venv/bin/activate  

# On Unix or MacOS

pip install -r requirements.txt


# Update your own SQL in pipeline.py:
  def __init__(self):
      self.conn = mysql.connector.connect(
          host = 'localhost',
          user = '<name of the user>',
          password = '< password if u have one>',
          database = '<the database u want to store into>'
      )
  # How to run your spider:
  1. go to your crawler file
  2. scrapy crawl [crawler name] // In this spider our crawler name is marketspider
  
