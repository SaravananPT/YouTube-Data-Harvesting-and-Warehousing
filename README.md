# YouTube Data Harvesting and Warehousing

YouTube Data Harvesting and Warehousing is a Python script that leverages the YouTube Data API to gather data from YouTube channels. It collects comprehensive information including channel details, video specifics, playlists, and comments. This script then stores the acquired data in MongoDB Atlas and AWS RDS MySQL databases for further analysis and querying.

## Features

- **Data Collection**: Gather extensive data from YouTube channels, including channel information, video details, playlists, and comments.
- **Data Warehousing**: Store harvested data in MongoDB Atlas and AWS RDS MySQL databases for easy access and querying.
- **SQL Query Execution**: Execute predefined SQL queries on the MySQL database to extract insights and perform data analysis.
- **User Interface**: Utilize a user-friendly Streamlit interface for seamless interaction and data exploration.

## Setup Instructions

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/your-username/youtube-data-harvesting-warehousing.git
   ```

2. **Install Dependencies**:

   ```bash
   cd youtube-data-harvesting-warehousing
   pip install -r requirements.txt
   ```

3. **Obtain API Keys and Database Connection Details**:

   - Acquire a YouTube Data API key from the [Google Cloud Console](https://console.cloud.google.com/).
   - Set up MongoDB Atlas and retrieve the connection URI.
   - Set up AWS RDS MySQL and obtain the host, username, password, and database name.

4. **Run the Script**:

   ```bash
   streamlit run main.py
   ```

5. **Interact with the Streamlit Interface**:

   - Input your API keys and database connection details.
   - Specify the channels to analyze and click on "Analyze Channels".
   - Choose queries from the sidebar to execute and view the results.

## Dependencies

- google-api-python-client
- pymongo
- mysql-connector-python
- pandas
- streamlit

## Contributions

Contributions are welcome! If you encounter any bugs or have suggestions for enhancements, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the MIT License. Refer to the [LICENSE](LICENSE) file for more information.

---
