🤖 AI-Powered SQL Query Assistant

Environment setup
1. Create a postgres database on render
2. Get the Gemini api key (free tier)

Deployement - 
1. install dependencies using requirement.txt
2. cretae ".streamlit" folder, with "secrets.toml" file in it. Store credentials in the ""secrets.toml"" file.
3. Download the "data.csv" and save it in the same working folder
4. Run "populate_database.py" to load the database using "data.csv" as source
5. Run "app1.py" (streamlit app script)
6. Push the folder in your github repo and connect the github with the streamlit app and deploy the app from streamlit. 