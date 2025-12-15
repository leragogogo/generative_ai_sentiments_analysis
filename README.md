# Predicting Societal Sentiment Towards Generative AI
This project analyzes and forecasts societal sentiment toward generative AI and its socio-economic implications 
using text data from YouTube comments and GDELT news articles. 


The study combines modern NLP techniques - transformer-based sentiment analysis, topic modeling, 
and time-series analysis—to examine how attitudes toward generative AI evolve over time.


The project was developed as part of a university final project and focuses on the period 2022–2025, 
covering the rapid adoption of tools such as ChatGPT, GPT-4, Stable Diffusion, and Midjourney.

## Setup Instructions
1. Clone the Repository
```python
git clone https://github.com/leragogogo/generative_ai_sentiments_analysis.git
cd generative_ai_sentiments_analysis
```
2. Create a Virtual Environment
```python
python -m venv venv
source venv/bin/activate   # macOS / Linux
# venv\Scripts\activate    # Windows
```
3. Install Dependencies
```python
pip install -r requirements.txt
```
5. API Keys
For YouTube scraping, create a .env file in the project root:
```python
YOUTUBE_API_KEY=your_api_key_here
```
## How to Run the Project
The recommended workflow is to execute the in order:
1. youtube_scraping.py
2. gdelt_scraping.py
3. preprocess.ipynb
4. sentiment.ipynb
5. topics_modeling.ipynb
6. forecasting_sentiment.ipynb
